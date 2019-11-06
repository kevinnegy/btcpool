/*
 The MIT License (MIT)

 Copyright (c) [2016] [BTC.COM]

 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:

 The above copyright notice and this permission notice shall be included in
 all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 THE SOFTWARE.
 */
#include "BlockMakerBitcoin.h"

#include "StratumBitcoin.h"
#include "BitcoinUtils.h"

#include "Utils.h"
#include "rsk/RskSolvedShareData.h"

#ifdef CHAIN_TYPE_ZEC
static inline CTransactionRef MakeTransactionRef() {
  return std::make_shared<const CTransaction>();
}
template <typename Tx>
static inline CTransactionRef MakeTransactionRef(Tx &&txIn) {
  return std::make_shared<const CTransaction>(std::forward<Tx>(txIn));
}
#else
#include <consensus/merkle.h>
#endif

#include <boost/thread.hpp>

#include <streams.h>

////////////////////////////////// BlockMaker //////////////////////////////////
BlockMakerBitcoin::BlockMakerBitcoin(
    shared_ptr<BlockMakerDefinition> blkMakerDef,
    const char *kafkaBrokers,
    const MysqlConnectInfo &poolDB)
  : BlockMaker(blkMakerDef, kafkaBrokers, poolDB)
  , kMaxRawGbtNum_(
        100) /* if 5 seconds a rawgbt, will hold 100*5/60 = 8 mins rawgbt */
  , kMaxStratumJobNum_(
        120) /* if 30 seconds a stratum job, will hold 60 mins stratum job */
  , lastSubmittedBlockTime()
  , submittedRskBlocks(0)
  , kafkaConsumerRawGbt_(
        kafkaBrokers, def()->rawGbtTopic_.c_str(), 0 /* patition */)
  // Selfish miner kafka producer
  , kafkaProducer_(
		  kafkaBrokers, def()->rawGbtTopic_.c_str(), 0 )
  , kafkaConsumerStratumJob_(
        kafkaBrokers, def()->stratumJobTopic_.c_str(), 0 /* patition */)
#ifndef CHAIN_TYPE_ZEC
  , kafkaConsumerNamecoinSolvedShare_(
        kafkaBrokers, def()->auxPowSolvedShareTopic_.c_str(), 0 /* patition */)
  , kafkaConsumerRskSolvedShare_(
        kafkaBrokers, def()->rskSolvedShareTopic_.c_str(), 0 /* patition */)
#endif
{
}

BlockMakerBitcoin::~BlockMakerBitcoin() {
  if (threadConsumeRawGbt_.joinable())
    threadConsumeRawGbt_.join();

  if (threadConsumeStratumJob_.joinable())
    threadConsumeStratumJob_.join();

#ifndef CHAIN_TYPE_ZEC
  if (threadConsumeNamecoinSolvedShare_.joinable())
    threadConsumeNamecoinSolvedShare_.join();

  if (threadConsumeRskSolvedShare_.joinable())
    threadConsumeRskSolvedShare_.join();
#endif
}

bool BlockMakerBitcoin::init() {
  if (!checkBitcoinds())
    return false;

  if (!BlockMaker::init()) {
    return false;
  }
  //
  // Raw Gbt
  //
  // Selfish miner kafka produce
  map<string, string> options;
  // set to 1 (0 is an illegal value here), deliver msg as soon as possible.
  options["queue.buffering.max.ms"] = "1";
  if (!kafkaProducer_.setup(&options)) {
    LOG(ERROR) << "kafka producer setup failure";
    return false;
  }

  printf("Checking if kafka is alive\n");
  // setup kafka and check if it's alive
  if (!kafkaProducer_.checkAlive()) {
    printf("kafka is NOT alive\n");
    LOG(ERROR) << "kafka is NOT alive";
    return false;
  }
  
  // we need to consume the latest N messages
  if (kafkaConsumerRawGbt_.setup(RD_KAFKA_OFFSET_TAIL(kMaxRawGbtNum_)) ==
      false) {
    LOG(INFO) << "setup kafkaConsumerRawGbt_ fail";
    return false;
  }
  if (!kafkaConsumerRawGbt_.checkAlive()) {
    LOG(ERROR) << "kafka brokers is not alive: kafkaConsumerRawGbt_";
    return false;
  }

  //
  // Stratum Job
  //
  // we need to consume the latest 2 messages, just in case
  if (kafkaConsumerStratumJob_.setup(
          RD_KAFKA_OFFSET_TAIL(kMaxStratumJobNum_)) == false) {
    LOG(INFO) << "setup kafkaConsumerStratumJob_ fail";
    return false;
  }
  if (!kafkaConsumerStratumJob_.checkAlive()) {
    LOG(ERROR) << "kafka brokers is not alive: kafkaConsumerStratumJob_";
    return false;
  }

#ifndef CHAIN_TYPE_ZEC
  //
  // Namecoin Sloved Share
  //
  // we need to consume the latest 2 messages, just in case
  if (kafkaConsumerNamecoinSolvedShare_.setup(RD_KAFKA_OFFSET_TAIL(2)) ==
      false) {
    LOG(INFO) << "setup kafkaConsumerNamecoinSolvedShare_ fail";
    return false;
  }
  if (!kafkaConsumerNamecoinSolvedShare_.checkAlive()) {
    LOG(ERROR)
        << "kafka brokers is not alive: kafkaConsumerNamecoinSolvedShare_";
    return false;
  }

  //
  // RSK Solved Share
  //
  // we need to consume the latest 2 messages, just in case
  if (kafkaConsumerRskSolvedShare_.setup(RD_KAFKA_OFFSET_TAIL(2)) == false) {
    LOG(INFO) << "setup kafkaConsumerRskSolvedShare_ fail";
    return false;
  }
  if (!kafkaConsumerRskSolvedShare_.checkAlive()) {
    LOG(ERROR) << "kafka brokers is not alive: kafkaConsumerRskSolvedShare_";
    return false;
  }
#endif

  return true;
}

void BlockMakerBitcoin::consumeRawGbt(rd_kafka_message_t *rkmessage) {
  // check error
  if (rkmessage->err) {
    if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
      // Reached the end of the topic+partition queue on the broker.
      // Not really an error.
      //      LOG(INFO) << "consumer reached end of " <<
      //      rd_kafka_topic_name(rkmessage->rkt)
      //      << "[" << rkmessage->partition << "] "
      //      << " message queue at offset " << rkmessage->offset;
      // acturlly
      return;
    }

    LOG(ERROR) << "consume error for topic "
               << rd_kafka_topic_name(rkmessage->rkt) << "["
               << rkmessage->partition << "] offset " << rkmessage->offset
               << ": " << rd_kafka_message_errstr(rkmessage);

    if (rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION ||
        rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC) {
      LOG(FATAL) << "consume fatal";
      stop();
    }
    return;
  }

  LOG(INFO) << "received rawgbt message, len: " << rkmessage->len;
  addRawgbt((const char *)rkmessage->payload, rkmessage->len);
}

// Precondition: private_chain must have at least one block
// Postcondition: returns true if jgbt needs to be stored in rawGbtMap
bool BlockMakerBitcoin::selfish_mine(JsonNode &jgbt){
	int block_height = jgbt["height"].uint32() -1;

	// new bitcoind block is several blocks behind our first SM block, return true to store in rawGbtMap
	if(private_chain.begin()->first - block_height > 1){
		return true;
	}

	// If last found block in gbt is one less than the first block in private_chain, either its our private_chain parent or a competing parent block
	if(private_chain.begin()->first - block_height == 1){
		return false ;
	}

	// Print first 3 blocks in private chain and their parent hash 
	LOG(INFO) << "Private chain size: " << private_chain.size();
	int i = 0;
	for(auto it = private_chain.begin(); it != private_chain.end(); it++){
		LOG(INFO) << it->first << ": " << it->second->GetHash().ToString() << " -> " << it->second->hashPrevBlock.ToString();
		i++;
		if(i == 3)
			break;
	}

	// Check if private chain has a block at the same height as new block 
	if(private_chain.find(block_height) != private_chain.end()){
		auto selfish_block_hash = private_chain[block_height]->GetHash();

		// If is our own block, only process if it hasn't been stored in rawGbtMap
		if(jgbt["previousblockhash"].str().compare(selfish_block_hash.ToString()) == 0){
			if(rawGbtMap_.find(selfish_block_hash) == rawGbtMap_.end())
				return true; 
			return false;
		}
	}

	int difference = private_chain.rbegin()->first - block_height;

	// Honest wins - erase private_chain
	if(difference <= -1){
		for(auto it = private_chain.begin(); it != private_chain.end(); it++){
			delete it->second;
		}	
		private_chain.empty();
	}

	// TODO: if submit results in rejected block, reset jobmaker
	else if(difference == 0){
		const string blockHex = EncodeHexBlock(*private_chain[block_height]);
		submitBlockNonBlocking(blockHex);
		private_chain.erase(block_height);
	}

	else if(difference == 1){
		if(private_chain.size() != 2){
			LOG(FATAL) << "Private chain is size " << private_chain.size() << " instead of 2\n";
		}

		for(auto it = private_chain.begin(); it != private_chain.end(); it++){
			const string blockHex = EncodeHexBlock(*it->second);
			submitBlockNonBlocking(blockHex);
			delete it->second;
		}

		private_chain.empty();
	}

	else{
		const string blockHex = EncodeHexBlock(*private_chain[block_height]);
		submitBlockNonBlocking(blockHex);
		private_chain.erase(block_height);
	}

	return false;
}

void BlockMakerBitcoin::addRawgbt(const char *str, size_t len) {
	JsonNode r;
	if (!JsonNode::parse(str, str + len, r)) {
		return;
	}
	if (r["created_at_ts"].type() != Utilities::JS::type::Int ||
	  r["block_template_base64"].type() != Utilities::JS::type::Str ||
	  r["gbthash"].type() != Utilities::JS::type::Str) {
		LOG(ERROR) << "invalid rawgbt: missing fields";
		return;
	}

	const uint256 gbtHash = uint256S(r["gbthash"].str());
	if (rawGbtMap_.find(gbtHash) != rawGbtMap_.end()) {
		LOG(ERROR) << "already exist raw gbt, ignore: " << gbtHash.ToString();
		return;
	}

	const string gbt = DecodeBase64(r["block_template_base64"].str());
	assert(gbt.length() > 64); // valid gbt string's len at least 64 bytes

	JsonNode nodeGbt;
	if (!JsonNode::parse(gbt.c_str(), gbt.c_str() + gbt.length(), nodeGbt)) {
		LOG(ERROR) << "parse gbt message to json fail";
		return;
	}
	JsonNode jgbt = nodeGbt["result"];
	
	// Only selfish mine if private chain has blocks
	if(private_chain.size() > 0){
		// selfish_mine returns false if block doesn't need to be stored in rawGbtMap
		if(selfish_mine(jgbt) == false){
			return;	
		}
	}


#ifdef CHAIN_TYPE_BCH
  bool isLightVersion = jgbt["job_id"].type() == Utilities::JS::type::Str;
  if (isLightVersion) {
    ScopeLock ls(rawGbtlightLock_);
    rawGbtlightMap_[gbtHash] = jgbt["job_id"].str();
    LOG(INFO) << "insert rawgbt light: " << gbtHash.ToString()
              << ", job_id: " << jgbt["job_id"].str().c_str();
    return;
  }
#endif // CHAIN_TYPE_BCH
  // transaction without coinbase_tx
  shared_ptr<vector<CTransactionRef>> vtxs =
      std::make_shared<vector<CTransactionRef>>();
  
  for (JsonNode &node : jgbt["transactions"].array()) {

#ifdef CHAIN_TYPE_ZEC
    CTransaction tx;
    DecodeHexTx(tx, node["data"].str());
    vtxs->push_back(MakeTransactionRef(tx));
#else
    CMutableTransaction tx;
    DecodeHexTx(tx, node["data"].str());
    vtxs->push_back(MakeTransactionRef(std::move(tx)));
#endif

  }

  LOG(INFO) << "insert rawgbt: " << gbtHash.ToString()
            << ", txs: " << vtxs->size();
  insertRawGbt(gbtHash, vtxs);
}

void BlockMakerBitcoin::insertRawGbt(
    const uint256 &gbtHash, shared_ptr<vector<CTransactionRef>> vtxs) {
  ScopeLock ls(rawGbtLock_);

  // insert rawgbt
  rawGbtMap_[gbtHash] = vtxs;
  rawGbtQ_.push_back(gbtHash);

  // remove rawgbt if need
  while (rawGbtQ_.size() > kMaxRawGbtNum_) {
    const uint256 h = *rawGbtQ_.begin();

    rawGbtMap_.erase(h); // delete from map
    rawGbtQ_.pop_front(); // delete from Q
  }
}

#ifndef CHAIN_TYPE_ZEC
static string _buildAuxPow(const CBlock *block) {
  //
  // see: https://en.bitcoin.it/wiki/Merged_mining_specification
  //
  string auxPow;

  //
  // build auxpow
  //
  // 1. coinbase hex
  {
    CDataStream ssTx(SER_NETWORK, PROTOCOL_VERSION);
    ssTx << block->vtx[0];
    auxPow += HexStr(ssTx.begin(), ssTx.end());
  }

  // 2. block_hash
#ifdef CHAIN_TYPE_LTC
  auxPow += block->GetPoWHash().GetHex();
#else
  auxPow += block->GetHash().GetHex();
#endif

  // 3. coinbase_branch, Merkle branch
  {
    vector<uint256> merkleBranch = BlockMerkleBranch(*block, 0 /* position */);

    // Number of links in branch
    // should be Variable integer, but can't over than 0xfd, so we just print
    // out 2 hex char
    // https://en.bitcoin.it/wiki/Protocol_specification#Variable_length_integer
    auxPow += Strings::Format("%02x", merkleBranch.size());

    // merkle branch
    for (auto &itr : merkleBranch) {
      // dump 32 bytes from memory
      string hex;
      Bin2Hex(itr.begin(), 32, hex);
      auxPow += hex;
    }

    // branch_side_mask is always going to be all zeroes, because the branch
    // hashes will always be "on the right" of the working hash
    auxPow += "00000000";
  }

  // 4. Aux Blockchain Link
  {
    auxPow += "00"; // Number of links in branch
    auxPow += "00000000"; // Branch sides bitmask
  }

  // 5. Parent Block Header
  {
    CDataStream ssBlock(SER_NETWORK, PROTOCOL_VERSION);
    ssBlock << block->GetBlockHeader();
    auxPow += HexStr(ssBlock.begin(), ssBlock.end());
  }

  return auxPow;
}

void BlockMakerBitcoin::consumeNamecoinSolvedShare(
    rd_kafka_message_t *rkmessage) {
  // check error
  if (rkmessage->err) {
    if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
      // Reached the end of the topic+partition queue on the broker.
      // Not really an error.
      //      LOG(INFO) << "consumer reached end of " <<
      //      rd_kafka_topic_name(rkmessage->rkt)
      //      << "[" << rkmessage->partition << "] "
      //      << " message queue at offset " << rkmessage->offset;
      // acturlly
      return;
    }

    LOG(ERROR) << "consume error for topic "
               << rd_kafka_topic_name(rkmessage->rkt) << "["
               << rkmessage->partition << "] offset " << rkmessage->offset
               << ": " << rd_kafka_message_errstr(rkmessage);

    if (rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION ||
        rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC) {
      LOG(FATAL) << "consume fatal";
      stop();
    }
    return;
  }

  LOG(INFO) << "received Namecoin SolvedShare message, len: " << rkmessage->len;

  //
  // namecoin solved share message
  //
  JsonNode j;
  if (JsonNode::parse(
          (const char *)rkmessage->payload,
          (const char *)rkmessage->payload + rkmessage->len,
          j) == false) {
    LOG(ERROR) << "decode namecoin solved share message fail: "
               << string((const char *)rkmessage->payload, rkmessage->len);
    return;
  }
  // check fields
  if (j["job_id"].type() != Utilities::JS::type::Int ||
      j["aux_block_hash"].type() != Utilities::JS::type::Str ||
      j["block_header"].type() != Utilities::JS::type::Str ||
      j["coinbase_tx"].type() != Utilities::JS::type::Str ||
      j["rpc_addr"].type() != Utilities::JS::type::Str ||
      j["rpc_userpass"].type() != Utilities::JS::type::Str) {
    LOG(ERROR) << "namecoin solved share message missing some fields";
    return;
  }

  const uint64_t jobId = j["job_id"].uint64();
  const string auxBlockHash = j["aux_block_hash"].str();
  const string blockHeaderHex = j["block_header"].str();
  const string coinbaseTxHex = j["coinbase_tx"].str();
  const string rpcAddr = j["rpc_addr"].str();
  const string rpcUserpass = j["rpc_userpass"].str();
  assert(blockHeaderHex.size() == sizeof(CBlockHeader) * 2);

  CBlockHeader blkHeader;
  vector<char> coinbaseTxBin;

  // block header, hex -> bin
  {
    vector<char> binOut;
    Hex2Bin(blockHeaderHex.c_str(), blockHeaderHex.length(), binOut);
    assert(binOut.size() == sizeof(CBlockHeader));
    memcpy((uint8_t *)&blkHeader, binOut.data(), binOut.size());
  }

  // coinbase tx, hex -> bin
  Hex2Bin(coinbaseTxHex.c_str(), coinbaseTxHex.length(), coinbaseTxBin);

  // get gbtHash and rawgbt (vtxs)
  uint256 gbtHash;
  shared_ptr<vector<CTransactionRef>> vtxs;
  {
    ScopeLock sl(jobIdMapLock_);
    if (jobId2GbtHash_.find(jobId) != jobId2GbtHash_.end()) {
      gbtHash = jobId2GbtHash_[jobId];
    }
  }

  {
    ScopeLock ls(rawGbtLock_);
    if (rawGbtMap_.find(gbtHash) == rawGbtMap_.end()) {
      LOG(ERROR) << "can't find this gbthash in rawGbtMap_: "
                 << gbtHash.ToString();
      return;
    }
    vtxs = rawGbtMap_[gbtHash];
    assert(vtxs.get() != nullptr);
  }

  //
  // build new block
  //
  CBlock newblk(blkHeader);

  // put coinbase tx
  {
    CSerializeData sdata;
    sdata.insert(sdata.end(), coinbaseTxBin.begin(), coinbaseTxBin.end());
    newblk.vtx.push_back(MakeTransactionRef());
    CDataStream c(sdata, SER_NETWORK, PROTOCOL_VERSION);
    c >> newblk.vtx[newblk.vtx.size() - 1];
  }

  // put other txs
  if (vtxs && vtxs->size()) {
    newblk.vtx.insert(newblk.vtx.end(), vtxs->begin(), vtxs->end());
  }

  //
  // build aux POW
  //
  const string auxPow = _buildAuxPow(&newblk);

  // submit to namecoind
  submitNamecoinBlockNonBlocking(
      auxBlockHash, auxPow, newblk.GetHash().ToString(), rpcAddr, rpcUserpass);
}

void BlockMakerBitcoin::submitNamecoinBlockNonBlocking(
    const string &auxBlockHash,
    const string &auxPow,
    const string &bitcoinBlockHash,
    const string &rpcAddress,
    const string &rpcUserpass) {
  // use thread to submit
  boost::thread t(boost::bind(
      &BlockMakerBitcoin::_submitNamecoinBlockThread,
      this,
      auxBlockHash,
      auxPow,
      bitcoinBlockHash,
      rpcAddress,
      rpcUserpass));
}

void BlockMakerBitcoin::_submitNamecoinBlockThread(
    const string &auxBlockHash,
    const string &auxPow,
    const string &bitcoinBlockHash,
    const string &rpcAddress,
    const string &rpcUserpass) {
  //
  // request : submitauxblock <hash> <auxpow>
  //
  {
    string request = "";

    bool isSupportSubmitAuxBlock = false;
    if (isAddrSupportSubmitAux_.find(rpcAddress) !=
        isAddrSupportSubmitAux_.end()) {
      isSupportSubmitAuxBlock =
          isAddrSupportSubmitAux_.find(rpcAddress)->second;
    } else {
      LOG(INFO) << "can't find " << rpcAddress
                << " in isAddrSupportSubmitAux_ map";
    }

    if (isSupportSubmitAuxBlock) {
      request = Strings::Format(
          "{\"id\":1,\"method\":\"submitauxblock\",\"params\":[\"%s\",\"%s\"]}",
          auxBlockHash,
          auxPow);
    } else {
      request = Strings::Format(
          "{\"id\":1,\"method\":\"getauxblock\",\"params\":[\"%s\",\"%s\"]}",
          auxBlockHash,
          auxPow);
    }

    DLOG(INFO) << "submitauxblock request: " << request;
    // try N times
    for (size_t i = 0; i < 3; i++) {
      string response;
      bool res = blockchainNodeRpcCall(
          rpcAddress.c_str(), rpcUserpass.c_str(), request.c_str(), response);

      // success
      if (res == true) {
        LOG(INFO) << "rpc call success, submit auxblock response: " << response;
        break;
      }

      // failure
      LOG(ERROR) << "rpc call fail: " << response;
    }
  }

  //
  // save to databse
  //
  if (!def()->foundAuxBlockTable_.empty()) {
    const string nowStr = date("%F %T");
    string sql;
    sql = Strings::Format(
        "INSERT INTO `%s` "
        " (`bitcoin_block_hash`,`aux_block_hash`,"
        "  `aux_pow`,`created_at`) "
        " VALUES (\"%s\",\"%s\",\"%s\",\"%s\"); ",
        def()->foundAuxBlockTable_.empty() ? "found_nmc_blocks"
                                           : def()->foundAuxBlockTable_,
        bitcoinBlockHash,
        auxBlockHash,
        auxPow,
        nowStr);
    // try connect to DB
    MySQLConnection db(poolDB_);
    for (size_t i = 0; i < 3; i++) {
      if (db.ping())
        break;
      else
        std::this_thread::sleep_for(3s);
    }

    if (db.execute(sql) == false) {
      LOG(ERROR) << "insert found block failure: " << sql;
    }
  }
}
#endif

bool BlockMakerBitcoin::bitcoindRpcGBT(string &response) {
  string request =
      "{\"jsonrpc\":\"1.0\",\"id\":\"1\",\"method\":\"getblocktemplate\","
      "\"params\":[{\"rules\" : [\"segwit\"]}]}";
  bool res = blockchainNodeRpcCall(
      def()->nodes.begin()->rpcAddr_.c_str(),
      def()->nodes.begin()->rpcUserPwd_.c_str(),
      request.c_str(),
      response);
  if (!res) {
    LOG(ERROR) << "bitcoind rpc failure";
    return false;
  }
  return true;
}
void find_and_replace(string &gbt, string find, string replace){
	auto iter = gbt.find(find);
	if(iter == string::npos){
		LOG(ERROR) << "Unable to find string in gbt: "<< find;
		return;
	} 

	iter = gbt.find(":", iter);
	iter++;
	gbt.insert(iter, replace + ","); 
	
	iter = gbt.find(replace);
	auto begin = gbt.find(",", iter);
	auto end = gbt.find(",", begin + 1);
	gbt.erase(begin,end-begin);

}
void empty_txs(string &gbt){
	auto iter = gbt.find("\"transactions\"");
	if(iter == string::npos){
		LOG(ERROR) << "Unable to find string in gbt: \"transactions\"";
		return;
	}	

	iter = gbt.find(":", iter);
	int counter = 0;	
	auto begin = gbt.find("[", iter);
	auto end = begin;
	begin++;
	counter++;
	while(counter > 0){
		end++;
		if (gbt[end] == ']'){
			counter--;		
		} 
		else if(gbt[end] == '['){
			counter++;	
		}	
		// Get block template (2019) doesn't contain triple nesting of brackets
		if(counter > 2){
			return;
		}
	}
	gbt.erase(begin, end-begin);
	return;

}
void modifyGbtWithSelfish(string &gbt, const CBlock * block, int height){
	find_and_replace(gbt, "\"previousblockhash\"", "\""+ block->GetHash().ToString() + "\""); 
	find_and_replace(gbt, "\"height\"", std::to_string(height+1)); 
	find_and_replace(gbt, "\"curtime\"", std::to_string(block->nTime)); 
	find_and_replace(gbt, "\"mintime\"", std::to_string(block->nTime)); 
	empty_txs(gbt);
}

string BlockMakerBitcoin::makeRawGbtMsg(const CBlock * block, int height){
  string gbt;
  if (!bitcoindRpcGBT(gbt)) {
    return "";
  }
  modifyGbtWithSelfish(gbt, block, height);
  

  JsonNode r;
  if (!JsonNode::parse(gbt.c_str(), gbt.c_str() + gbt.length(), r)) {
    LOG(ERROR) << "decode gbt failure: " << gbt;
    return "";
  }

  // check fields
  if (r["result"].type() != Utilities::JS::type::Obj ||
      r["result"]["previousblockhash"].type() != Utilities::JS::type::Str ||
      r["result"]["height"].type() != Utilities::JS::type::Int ||
#ifdef CHAIN_TYPE_ZEC
      r["result"]["coinbasetxn"].type() != Utilities::JS::type::Obj ||
      r["result"]["coinbasetxn"]["data"].type() != Utilities::JS::type::Str ||
      r["result"]["coinbasetxn"]["fee"].type() != Utilities::JS::type::Int ||
#else
      r["result"]["coinbasevalue"].type() != Utilities::JS::type::Int ||
#endif
      r["result"]["bits"].type() != Utilities::JS::type::Str ||
      r["result"]["mintime"].type() != Utilities::JS::type::Int ||
      r["result"]["curtime"].type() != Utilities::JS::type::Int ||
      r["result"]["version"].type() != Utilities::JS::type::Int) {
    LOG(ERROR) << "gbt check fields failure";
    return "";
  }
  const uint256 gbtHash = Hash(gbt.begin(), gbt.end());

  LOG(INFO) << "gbt height: " << r["result"]["height"].uint32()
            << ", prev_hash: " << r["result"]["previousblockhash"].str()
#ifdef CHAIN_TYPE_ZEC
            << ", coinbase_fee: "
            << r["result"]["coinbasevalue"]["fee"].uint64()
#else
            << ", coinbase_value: " << r["result"]["coinbasevalue"].uint64()
#endif
            << ", bits: " << r["result"]["bits"].str()
            << ", mintime: " << r["result"]["mintime"].uint32()
            << ", version: " << r["result"]["version"].uint32() << "|0x"
            << Strings::Format("%08x", r["result"]["version"].uint32())
            << ", gbthash: " << gbtHash.ToString();

  return Strings::Format(
      "{\"created_at_ts\":%u,"
      "\"block_template_base64\":\"%s\","
      "\"gbthash\":\"%s\"}",
      (uint32_t)time(nullptr),
      EncodeBase64(gbt),
      gbtHash.ToString());
  //  return Strings::Format("{\"created_at_ts\":%u,"
  //                         "\"gbthash\":\"%s\"}",
  //                         (uint32_t)time(nullptr),
  //                         gbtHash.ToString());

}

void BlockMakerBitcoin::processSolvedShare(rd_kafka_message_t *rkmessage) {
  LOG(INFO) << "Process solved share:";
  //
  // solved share message:  FoundBlock + coinbase_Tx
  //
  FoundBlock foundBlock;
  CBlockHeader blkHeader;
  vector<char> coinbaseTxBin;

  {
    if (rkmessage->len <= sizeof(FoundBlock)) {
      LOG(ERROR) << "invalid SolvedShare length: " << rkmessage->len;
      return;
    }
    coinbaseTxBin.resize(rkmessage->len - sizeof(FoundBlock));

    // foundBlock
    memcpy(
        (uint8_t *)&foundBlock,
        (const uint8_t *)rkmessage->payload,
        sizeof(FoundBlock));

    // coinbase tx
    memcpy(
        (uint8_t *)coinbaseTxBin.data(),
        (const uint8_t *)rkmessage->payload + sizeof(FoundBlock),
        coinbaseTxBin.size());
    // copy header
    foundBlock.headerData_.get(blkHeader);
  }

  // get gbtHash and rawgbt (vtxs)
  uint256 gbtHash;
  shared_ptr<vector<CTransactionRef>> vtxs;
  {
    ScopeLock sl(jobIdMapLock_);
    if (jobId2GbtHash_.find(foundBlock.jobId_) != jobId2GbtHash_.end()) {
      gbtHash = jobId2GbtHash_[foundBlock.jobId_];
    }
  }

#ifdef CHAIN_TYPE_BCH
  std::string gbtlightJobId;
  {
    ScopeLock ls(rawGbtlightLock_);
    const auto iter = rawGbtlightMap_.find(gbtHash);
    if (iter != rawGbtlightMap_.end()) {
      gbtlightJobId = iter->second;
    }
  }
  bool lightVersion = !gbtlightJobId.empty();
  if (!lightVersion)
#endif // CHAIN_TYPE_BCH
  {
    ScopeLock ls(rawGbtLock_);
    if (rawGbtMap_.find(gbtHash) == rawGbtMap_.end()) {
      LOG(ERROR) << "can't find this gbthash in rawGbtMap_: "
                 << gbtHash.ToString();
      return;
    }
    vtxs = rawGbtMap_[gbtHash];
    assert(vtxs.get() != nullptr);
  }

  //
  // build new block
  //
  CBlock * newblk = new CBlock(blkHeader);


  // put coinbase tx
  {
    CSerializeData sdata;
    sdata.insert(sdata.end(), coinbaseTxBin.begin(), coinbaseTxBin.end());
#ifdef CHAIN_TYPE_ZEC
    newblk->vtx.push_back(CTransaction());
#else
    newblk->vtx.push_back(MakeTransactionRef());
#endif
    CDataStream c(sdata, SER_NETWORK, PROTOCOL_VERSION);
    c >> newblk->vtx[newblk->vtx.size() - 1];
  }

  // put other txs
  if (vtxs && vtxs->size()) {
#ifdef CHAIN_TYPE_ZEC
    for (size_t i = 0; i < vtxs->size(); ++i) {
      newblk->vtx.push_back(*vtxs->at(i));
    }
#else
    newblk->vtx.insert(newblk->vtx.end(), vtxs->begin(), vtxs->end());
#endif
  }
  
  const string blockHex = EncodeHexBlock(*newblk);

  // Don't store block if we already have a block at that height
  int height = (int)foundBlock.height_;
  if(private_chain.find(height) != private_chain.end()){
  	return;
  }

  // Store in private chain and submit to kafka producer new gbt with our hash
  private_chain[height]= newblk;
  const string rawGbtMsg = makeRawGbtMsg(newblk, height);
  kafkaProducer_.produce(rawGbtMsg.data(), rawGbtMsg.size());

// Honest mining: submit to bitcoind
//#ifdef CHAIN_TYPE_BCH
//  if (lightVersion) {
//    LOG(INFO) << "submit block light: " << newblk->GetHash().ToString()
//              << " with job_id: " << gbtlightJobId.c_str();
//    submitBlockLightNonBlocking(blockHex, gbtlightJobId);
//  } else
//#endif // CHAIN_TYPE_BCH
//  {
//#ifdef CHAIN_TYPE_LTC
//    LOG(INFO) << "submit block pow: " << newblk->GetPoWHash().ToString();
//#endif
//    LOG(INFO) << "submit block: " << newblk->GetHash().ToString();
//    submitBlockNonBlocking(blockHex); // using thread
//  }

#ifdef CHAIN_TYPE_ZEC
  uint64_t coinbaseValue = AMOUNT_SATOSHIS(newblk->vtx[0].GetValueOut());
#else
  uint64_t coinbaseValue = AMOUNT_SATOSHIS(newblk->vtx[0]->GetValueOut());
#endif

  // save to DB, using thread
  saveBlockToDBNonBlocking(
      foundBlock,
      blkHeader,
      coinbaseValue, // coinbase value
      blockHex.length() / 2);
}

void BlockMakerBitcoin::saveBlockToDBNonBlocking(
    const FoundBlock &foundBlock,
    const CBlockHeader &header,
    const uint64_t coinbaseValue,
    const int32_t blksize) {
  boost::thread t(boost::bind(
      &BlockMakerBitcoin::_saveBlockToDBThread,
      this,
      foundBlock,
      header,
      coinbaseValue,
      blksize));
}

void BlockMakerBitcoin::_saveBlockToDBThread(
    const FoundBlock &foundBlock,
    const CBlockHeader &header,
    const uint64_t coinbaseValue,
    const int32_t blksize) {
  const string nowStr = date("%F %T");
  string sql;
  sql = Strings::Format(
      "INSERT INTO `found_blocks` "
      " (`puid`, `worker_id`, `worker_full_name`, `job_id`"
      "  ,`height`, `hash`, `rewards`, `size`, `prev_hash`"
      "  ,`bits`, `version`, `created_at`)"
      " VALUES (%d,%d,\"%s\",%u,%d,\"%s\",%d,%d,\"%s\",%u,%d,\"%s\"); ",
      foundBlock.userId_,
      foundBlock.workerId_,
      // filter again, just in case
      filterWorkerName(foundBlock.workerFullName_),
      foundBlock.jobId_,
      foundBlock.height_,
      header.GetHash().ToString(),
      coinbaseValue,
      blksize,
      header.hashPrevBlock.ToString(),
      header.nBits,
      header.nVersion,
      nowStr);

  LOG(INFO) << "BlockMakerBitcoin::_saveBlockToDBThread: " << sql;

  // try connect to DB
  MySQLConnection db(poolDB_);
  for (size_t i = 0; i < 3; i++) {
    if (db.ping())
      break;
    else
      std::this_thread::sleep_for(3s);
  }

  if (db.execute(sql) == false) {
    LOG(ERROR) << "insert found block failure: " << sql;
  }
}

bool BlockMakerBitcoin::checkBitcoinds() {
  if (def()->nodes.size() == 0) {
    return false;
  }

  for (const auto &itr : def()->nodes) {
    if (!checkBitcoinRPC(itr.rpcAddr_.c_str(), itr.rpcUserPwd_.c_str())) {
      return false;
    }
  }

  return true;
}

void BlockMakerBitcoin::submitBlockNonBlocking(const string &blockHex) {
  for (const auto &itr : def()->nodes) {
    // use thread to submit
    boost::thread t(boost::bind(
        &BlockMakerBitcoin::_submitBlockThread,
        this,
        itr.rpcAddr_,
        itr.rpcUserPwd_,
        blockHex));
  }
}

void BlockMakerBitcoin::_submitBlockThread(
    const string &rpcAddress,
    const string &rpcUserpass,
    const string &blockHex) {
  string request =
      "{\"jsonrpc\":\"1.0\",\"id\":\"1\",\"method\":\"submitblock\",\"params\":"
      "[\"";
  request += blockHex + "\"]}";

  LOG(INFO) << "submit block to: " << rpcAddress;
  DLOG(INFO) << "submitblock request: " << request;
  // try N times
  for (size_t i = 0; i < 3; i++) {
    string response;
    bool res = blockchainNodeRpcCall(
        rpcAddress.c_str(), rpcUserpass.c_str(), request.c_str(), response);

    // success
    if (res == true) {
      LOG(INFO) << "rpc call success, submit block response: " << response;
      break;
    }

    // failure
    LOG(ERROR) << "rpc call fail: " << response;
  }
}

#ifdef CHAIN_TYPE_BCH
void BlockMakerBitcoin::submitBlockLightNonBlocking(
    const string &blockHex, const string &job_id) {
  for (const auto &itr : def()->nodes) {
    // use thread to submit
    boost::thread t(boost::bind(
        &BlockMakerBitcoin::_submitBlockLightThread,
        this,
        itr.rpcAddr_,
        itr.rpcUserPwd_,
        job_id,
        blockHex));
    t.detach();
  }
}
void BlockMakerBitcoin::_submitBlockLightThread(
    const string &rpcAddress,
    const string &rpcUserpass,
    const string &job_id,
    const string &blockHex) {

  string request =
      "{\"jsonrpc\":\"1.0\",\"id\":\"1\",\"method\":\"submitblocklight\","
      "\"params\":[\"";
  request += blockHex + "\", \"";
  request += job_id + "\"";
  request += "]}";
  LOG(INFO) << "submit block light to: " << rpcAddress;
  DLOG(INFO) << "submitblock request: " << request;
  // try N times
  for (size_t i = 0; i < 3; i++) {
    string response;
    bool res = blockchainNodeRpcCall(
        rpcAddress.c_str(), rpcUserpass.c_str(), request.c_str(), response);
    // success
    if (res == true) {
      LOG(INFO) << "rpc call success, submit block light response: "
                << response;
      break;
    }
    // failure
    LOG(ERROR) << "rpc call fail: " << response;
  }
}
#endif // CHAIN_TYPE_BCH

void BlockMakerBitcoin::consumeStratumJob(rd_kafka_message_t *rkmessage) {
  // check error
  if (rkmessage->err) {
    if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
      // Reached the end of the topic+partition queue on the broker.
      // Not really an error.
      //      LOG(INFO) << "consumer reached end of " <<
      //      rd_kafka_topic_name(rkmessage->rkt)
      //      << "[" << rkmessage->partition << "] "
      //      << " message queue at offset " << rkmessage->offset;
      // acturlly
      return;
    }

    LOG(ERROR) << "consume error for topic "
               << rd_kafka_topic_name(rkmessage->rkt) << "["
               << rkmessage->partition << "] offset " << rkmessage->offset
               << ": " << rd_kafka_message_errstr(rkmessage);

    if (rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION ||
        rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC) {
      LOG(FATAL) << "consume fatal";
      stop();
    }
    return;
  }

  LOG(INFO) << "received StratumJob message, len: " << rkmessage->len;

  shared_ptr<StratumJobBitcoin> sjob = std::make_shared<StratumJobBitcoin>();
  bool res = sjob->unserializeFromJson(
      (const char *)rkmessage->payload, rkmessage->len);
  if (res == false) {
    LOG(ERROR) << "unserialize stratum job fail";
    return;
  }

  const uint256 gbtHash = uint256S(sjob->gbtHash_);
  {
    ScopeLock sl(jobIdMapLock_);
    jobId2GbtHash_[sjob->jobId_] = gbtHash;

    // Maps (and sets) are sorted, so the first element is the smallest,
    // and the last element is the largest.
    while (jobId2GbtHash_.size() > kMaxStratumJobNum_) {
      jobId2GbtHash_.erase(jobId2GbtHash_.begin());
    }
  }

  LOG(INFO) << "StratumJob, jobId: " << sjob->jobId_
            << ", gbtHash: " << gbtHash.ToString();

#ifndef CHAIN_TYPE_ZEC
  bool isSupportSubmitAuxBlock = false;
  if (!sjob->nmcRpcAddr_.empty() && !sjob->nmcRpcUserpass_.empty() &&
      isAddrSupportSubmitAux_.find(sjob->nmcRpcAddr_) ==
          isAddrSupportSubmitAux_.end()) {

    string response;
    string request =
        "{\"jsonrpc\":\"1.0\",\"id\":\"1\",\"method\":\"help\",\"params\":[]}";
    bool res = blockchainNodeRpcCall(
        sjob->nmcRpcAddr_.c_str(),
        sjob->nmcRpcUserpass_.c_str(),
        request.c_str(),
        response);
    if (!res) {
      LOG(INFO) << "auxcoind rpc call failure";
    } else {
      isSupportSubmitAuxBlock =
          (response.find("createauxblock") == std::string::npos ||
           response.find("submitauxblock") == std::string::npos)
          ? false
          : true;

      LOG(INFO) << "auxcoind " << (isSupportSubmitAuxBlock ? " " : "doesn't ")
                << "support rpc commands: createauxblock and submitauxblock";

      isAddrSupportSubmitAux_[sjob->nmcRpcAddr_] = isSupportSubmitAuxBlock;
    }
  }
#endif
}

void BlockMakerBitcoin::runThreadConsumeRawGbt() {
  const int32_t timeoutMs = 1000;

  while (running_) {
    rd_kafka_message_t *rkmessage;
    rkmessage = kafkaConsumerRawGbt_.consumer(timeoutMs);
    if (rkmessage == nullptr) /* timeout */
      continue;

    consumeRawGbt(rkmessage);

    /* Return message to rdkafka */
    rd_kafka_message_destroy(rkmessage);
  }
}

void BlockMakerBitcoin::runThreadConsumeStratumJob() {
  const int32_t timeoutMs = 1000;

  while (running_) {
    rd_kafka_message_t *rkmessage;
    rkmessage = kafkaConsumerStratumJob_.consumer(timeoutMs);
    if (rkmessage == nullptr) /* timeout */
      continue;

    consumeStratumJob(rkmessage);

    /* Return message to rdkafka */
    rd_kafka_message_destroy(rkmessage);
  }
}

#ifndef CHAIN_TYPE_ZEC
void BlockMakerBitcoin::runThreadConsumeNamecoinSolvedShare() {
  const int32_t timeoutMs = 1000;

  while (running_) {
    rd_kafka_message_t *rkmessage;
    rkmessage = kafkaConsumerNamecoinSolvedShare_.consumer(timeoutMs);
    if (rkmessage == nullptr) /* timeout */
      continue;

    consumeNamecoinSolvedShare(rkmessage);

    /* Return message to rdkafka */
    rd_kafka_message_destroy(rkmessage);
  }
}

/**
  Beginning of methods needed to consume a solved share and submit a block to
  RSK node.

  @author Martin Medina
  @copyright RSK Labs Ltd.
*/
void BlockMakerBitcoin::submitRskBlockPartialMerkleNonBlocking(
    const string &rpcAddress,
    const string &rpcUserPwd,
    const string &blockHashHex,
    const string &blockHeaderHex,
    const string &coinbaseHex,
    const string &merkleHashesHex,
    const string &totalTxCount) {
  boost::thread t(boost::bind(
      &BlockMakerBitcoin::_submitRskBlockPartialMerkleThread,
      this,
      rpcAddress,
      rpcUserPwd,
      blockHashHex,
      blockHeaderHex,
      coinbaseHex,
      merkleHashesHex,
      totalTxCount));
}

void BlockMakerBitcoin::_submitRskBlockPartialMerkleThread(
    const string &rpcAddress,
    const string &rpcUserPwd,
    const string &blockHashHex,
    const string &blockHeaderHex,
    const string &coinbaseHex,
    const string &merkleHashesHex,
    const string &totalTxCount) {
  string request =
      "{\"jsonrpc\":\"2.0\",\"id\":\"1\",\"method\":\"mnr_"
      "submitBitcoinBlockPartialMerkle\",\"params\":[";
  request += "\"" + blockHashHex + "\", ";
  request += "\"" + blockHeaderHex + "\", ";
  request += "\"" + coinbaseHex + "\", ";
  request += "\"" + merkleHashesHex + "\", ";
  request += "\"" + totalTxCount + "\"]}";

  LOG(INFO) << "submit block to: " << rpcAddress;
  // try N times
  for (size_t i = 0; i < 3; i++) {
    string response;
    bool res = blockchainNodeRpcCall(
        rpcAddress.c_str(), rpcUserPwd.c_str(), request.c_str(), response);

    // success
    if (res) {
      LOG(INFO) << "rpc call success, submit block response: " << response;
      break;
    }

    // failure
    LOG(ERROR) << "rpc call fail: " << response;
  }
}

void BlockMakerBitcoin::consumeRskSolvedShare(rd_kafka_message_t *rkmessage) {
  // check error
  if (rkmessage->err) {
    if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
      // Reached the end of the topic+partition queue on the broker.
      return;
    }

    LOG(ERROR) << "consume error for topic "
               << rd_kafka_topic_name(rkmessage->rkt) << "["
               << rkmessage->partition << "] offset " << rkmessage->offset
               << ": " << rd_kafka_message_errstr(rkmessage);

    if (rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION ||
        rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC) {
      LOG(FATAL) << "consume fatal";
      stop();
    }
    return;
  }

  LOG(INFO) << "received RskSolvedShareData message, len: " << rkmessage->len;

  if (!submitToRskNode()) {
    return;
  }

  //
  // solved share message:  RskSolvedShareData + coinbase_Tx
  //
  RskSolvedShareData shareData;
  CBlockHeader blkHeader;
  vector<char> coinbaseTxBin;
  {
    if (rkmessage->len <= sizeof(RskSolvedShareData)) {
      LOG(ERROR) << "invalid RskSolvedShareData length: " << rkmessage->len;
      return;
    }
    coinbaseTxBin.resize(rkmessage->len - sizeof(RskSolvedShareData));

    // shareData
    memcpy(
        (uint8_t *)&shareData,
        (const uint8_t *)rkmessage->payload,
        sizeof(RskSolvedShareData));
    // coinbase tx
    memcpy(
        (uint8_t *)coinbaseTxBin.data(),
        (const uint8_t *)rkmessage->payload + sizeof(RskSolvedShareData),
        coinbaseTxBin.size());
    // copy header
    shareData.headerData_.get(blkHeader);
  }

  LOG(INFO) << "submit RSK block: " << blkHeader.GetHash().ToString();

  // get gbtHash and rawgbt (vtxs)
  uint256 gbtHash;
  shared_ptr<vector<CTransactionRef>> vtxs;
  {
    ScopeLock sl(jobIdMapLock_);
    if (jobId2GbtHash_.find(shareData.jobId_) != jobId2GbtHash_.end()) {
      gbtHash = jobId2GbtHash_[shareData.jobId_];
    }
  }
  {
    ScopeLock ls(rawGbtLock_);
    if (rawGbtMap_.find(gbtHash) == rawGbtMap_.end()) {
      LOG(ERROR) << "can't find this gbthash in rawGbtMap_: "
                 << gbtHash.ToString();
      return;
    }
    vtxs = rawGbtMap_[gbtHash];
  }
  assert(vtxs.get() != nullptr);

  vector<uint256> vtxhashes;
  vtxhashes.resize(1 + vtxs->size()); // coinbase + gbt txs

  // put coinbase tx hash
  {
    CSerializeData sdata;
    sdata.insert(sdata.end(), coinbaseTxBin.begin(), coinbaseTxBin.end());

    CMutableTransaction tx;
    CDataStream c(sdata, SER_NETWORK, PROTOCOL_VERSION);
    c >> tx;

    vtxhashes[0] = tx.GetHash();
  }

  // put other tx hashes
  for (size_t i = 0; i < vtxs->size(); i++) {
    vtxhashes[i + 1] =
        (*vtxs)[i]->GetHash(); // vtxs is a shared_ptr<vector<CTransactionRef>>
  }

  string blockHashHex = blkHeader.GetHash().ToString();
  string blockHeaderHex = EncodeHexBlockHeader(blkHeader);

  // coinbase bin -> hex
  string coinbaseHex;
  Bin2Hex(coinbaseTxBin, coinbaseHex);

  // build coinbase's merkle tree branch
  string merkleHashesHex;
  string hashHex;
  vector<uint256> cbMerkleBranch = ComputeMerkleBranch(vtxhashes, 0);

  Bin2Hex(
      (uint8_t *)(vtxhashes[0].begin()),
      sizeof(uint256),
      hashHex); // coinbase hash
  merkleHashesHex.append(hashHex);
  for (size_t i = 0; i < cbMerkleBranch.size(); i++) {
    merkleHashesHex.append("\x20"); // space character
    Bin2Hex((uint8_t *)cbMerkleBranch[i].begin(), sizeof(uint256), hashHex);
    merkleHashesHex.append(hashHex);
  }

  // block tx count
  std::stringstream sstream;
  sstream << std::hex << vtxhashes.size();
  string totalTxCountHex(sstream.str());

  submitRskBlockPartialMerkleNonBlocking(
      shareData.rpcAddress_,
      shareData.rpcUserPwd_,
      blockHashHex,
      blockHeaderHex,
      coinbaseHex,
      merkleHashesHex,
      totalTxCountHex); // using thread
}

/**
  Anti flooding mechanism.
  No more than 2 submissions per second can be made to RSK node.

  @returns true if block can be submitted to RSK node. false otherwise.
*/
bool BlockMakerBitcoin::submitToRskNode() {
  uint32_t maxSubmissionsPerSecond = 2;
  int64_t oneSecondWindowInMs = 1000;

  if (lastSubmittedBlockTime.is_not_a_date_time()) {
    lastSubmittedBlockTime = bpt::microsec_clock::universal_time();
  }

  bpt::ptime currentTime(bpt::microsec_clock::universal_time());
  bpt::time_duration elapsed = currentTime - lastSubmittedBlockTime;

  if (elapsed.total_milliseconds() > oneSecondWindowInMs) {
    lastSubmittedBlockTime = currentTime;
    submittedRskBlocks = 0;
    elapsed = currentTime - lastSubmittedBlockTime;
  }

  if (elapsed.total_milliseconds() < oneSecondWindowInMs &&
      submittedRskBlocks < maxSubmissionsPerSecond) {
    submittedRskBlocks++;
    return true;
  }

  return false;
}

void BlockMakerBitcoin::runThreadConsumeRskSolvedShare() {
  const int32_t timeoutMs = 1000;

  while (running_) {
    rd_kafka_message_t *rkmessage;
    rkmessage = kafkaConsumerRskSolvedShare_.consumer(timeoutMs);
    if (rkmessage == nullptr) /* timeout */
      continue;

    consumeRskSolvedShare(rkmessage);

    /* Return message to rdkafka */
    rd_kafka_message_destroy(rkmessage);
  }
}
//// End of methods added to merge mine for RSK
#endif

void BlockMakerBitcoin::run() {
  // setup threads
  threadConsumeRawGbt_ =
      thread(&BlockMakerBitcoin::runThreadConsumeRawGbt, this);
  threadConsumeStratumJob_ =
      thread(&BlockMakerBitcoin::runThreadConsumeStratumJob, this);
#ifndef CHAIN_TYPE_ZEC
  threadConsumeNamecoinSolvedShare_ =
      thread(&BlockMakerBitcoin::runThreadConsumeNamecoinSolvedShare, this);
  threadConsumeRskSolvedShare_ =
      thread(&BlockMakerBitcoin::runThreadConsumeRskSolvedShare, this);
#endif
  BlockMaker::run();
}
