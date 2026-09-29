SELECT toDate(dt) d, count() rows, uniqExact(blockNumber) blocks, min(blockNumber) bmin, max(blockNumber) bmax,
       uniqExact(cityHash64(transactionHash)) txs, uniqExact(kh) keys, sumDistinct(ch) fp
FROM (SELECT dt, blockNumber, transactionHash,
             cityHash64(dt, assetRefId, address, blockNumber, transactionIndex) kh,
             cityHash64(kh, bitShiftRight(reinterpretAsUInt64(balance), 20),
                        isNull(oldBalance), bitShiftRight(reinterpretAsUInt64(ifNull(oldBalance, 0)), 20),
                        ifNull(oldBlockNumber, 0), ifNull(oldDt, toDateTime(0))) ch
      FROM TABLE WHERE dt >= '2024-01-01' AND dt < '2024-01-03')
GROUP BY d ORDER BY d
