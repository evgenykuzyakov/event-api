const MainnetUrl = "https://mainnet.neardata.xyz/v0";
const EventLogPrefix = "EVENT_JSON:";
const FetchTimeoutStart = 2000;
const FetchTimeoutIncrease = 500;

const ReceiptStatus = {
  Success: "SUCCESS",
  Failure: "FAILURE",
};

function timeoutPromise(time, abortController) {
  return new Promise((resolve, reject) => {
    setTimeout(() => {
      abortController.abort();
      return reject(`Timeout: ${time}ms exceeded`);
    }, time);
  });
}

const fetchBlock = async (blockHeight) => {
  const iters = 10;
  let timeout = 100;
  let fetchTimeout = FetchTimeoutStart;
  const jsonFetch = async (url, abortController) => {
    const start = Date.now();
    const response = await fetch(url, { signal: abortController.signal });
    const text = await response.text();
    const elapsed = Date.now() - start;
    console.log(`Fetched ${text.length} bytes. Elapsed: ${elapsed}ms`);
    return JSON.parse(text);
  };
  for (let i = 0; i < iters; i++) {
    try {
      const url = `${MainnetUrl}/block/${blockHeight}`;
      const abortController = new AbortController();
      console.log("Fetching block", url);
      return await Promise.race([
        jsonFetch(url, abortController),
        timeoutPromise(fetchTimeout, abortController),
      ]);
    } catch (e) {
      console.error("Failed to fetch block", blockHeight, e);
      await new Promise((r) => setTimeout(r, timeout));
      fetchTimeout += FetchTimeoutIncrease;
      timeout *= 2;
    }
  }
  return null;
};

const Fetcher = {
  init: async function (lastBlockHeight) {
    if (!lastBlockHeight) {
      const query = await fetch(`${MainnetUrl}/last_block/final`);
      const block = await query.json();
      lastBlockHeight = block.block.header.height;
    }
    this.lastBlockHeight = lastBlockHeight;
    return this;
  },

  processBlock: function (block) {
    const res = {
      events: [],
      actions: [],
    };
    if (!block) {
      return res;
    }
    const blockHeight = block.block.header.height;
    const blockHash = block.block.header.hash;
    const blockTimestampNs = block.block.header.timestamp_nanosec;
    const blockTimestampMs = parseFloat(blockTimestampNs) / 1e6;
    let receiptIndex = 0;
    for (const shard of block.shards) {
      const shardId = shard.shard_id;
      for (const outcome of shard.receipt_execution_outcomes) {
        const txHash = outcome.tx_hash;
        const {
          predecessor_id: predecessorId,
          receiver_id: accountId,
          receipt_id: receiptId,
          receipt,
        } = outcome.receipt;
        const {
          status: executionStatus,
          gas_burnt: gasBurnt,
          tokens_burnt: tokensBurnt,
          logs,
        } = outcome.execution_outcome.outcome;
        const status =
          "SuccessValue" in executionStatus ||
          "SuccessReceiptId" in executionStatus
            ? ReceiptStatus.Success
            : ReceiptStatus.Failure;
        if ("Action" in receipt) {
          const {
            signer_id: signerId,
            signer_public_key: signerPublicKey,
            actions,
            gas_price: gasPrice,
          } = receipt.Action;
          // Parse logs
          for (let logIndex = 0; logIndex < logs.length; logIndex++) {
            const log = logs[logIndex];
            if (log.startsWith(EventLogPrefix)) {
              let event = null;
              try {
                event = JSON.parse(log.slice(EventLogPrefix.length));
              } catch (e) {
                console.debug("Failed to parse event log", e);
              }
              res.events.push({
                blockHeight,
                blockHash,
                blockTimestampMs,
                blockTimestampNs,
                shardId,
                txHash,
                receiptId,
                signerId,
                signerPublicKey,
                accountId,
                predecessorId,
                status,
                logIndex,
                event,
              });
            }
          }

          // Parse actions
          for (
            let actionIndex = 0;
            actionIndex < actions.length;
            actionIndex++
          ) {
            const action = actions[actionIndex];
            res.actions.push({
              blockHeight,
              blockHash,
              blockTimestampMs,
              blockTimestampNs,
              shardId,
              txHash,
              receiptId,
              signerId,
              signerPublicKey,
              accountId,
              predecessorId,
              status,
              gasBurnt,
              tokensBurnt,
              gasPrice,
              actionIndex,
              action,
            });
          }
        }
        ++receiptIndex;
      }
    }
    return res;
  },

  fetchNextBlock: async function () {
    const block = await fetchBlock(this.lastBlockHeight + 1);
    const res = this.processBlock(block);
    this.lastBlockHeight++;
    return res;
  },
};

module.exports = Fetcher;
