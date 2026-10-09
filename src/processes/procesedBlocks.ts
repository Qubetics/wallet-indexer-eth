import { blockChainHelper } from "../helpers/index";
import { config } from "../config/config";
import ethProcessHelper from "./process.helper";
import { KEYS } from "../enum";
import { blocksTrnx } from "../interfaces/globalHelper";
import { ethOldBLockModel } from "../models";
import { globalHelper } from "../helpers/global.helper";
import kafkaHelper from "../config/kafka.helper";
import { ethers } from "ethers";


class EthBlockProcess {
    public minBlockConfirmation: number;
    public swap: string | null;
    public approval: string | null;
    public eth_block_tx_topic = config.KAFKA_BLOCKS.ETH_BLOCK_TX_TOPIC;
    public eth_behind_block_tx_topic = config.KAFKA_BLOCKS.ETH_BEHIND_BLOCK_TX_TOPIC;

    constructor() {
        this.minBlockConfirmation = Number(config.MIN_BLOCK_CONFORMATIONS);
        this.swap = ethers.id('Swap(address,uint256,uint256,uint256,uint256,address)');
        this.approval = ethers.id('Approval(address,address,uint256)');
    }

    public async readCurrentBlocks() {
        try {
            const results = await ethProcessHelper.getMultipleKeyValuePairs(config.REDISKEYS.ETH_BLOCKS, [config.REDISKEYS.OLD_BLOCK_ETH, config.REDISKEYS.NEW_BLOCK_ETH]);

            // Validate results array
            if (!Array.isArray(results) || results.length < 2) {
                console.error("Invalid results from Redis operation:", results);
                return;
            }
            const [old_block, new_block] = results;
            if (!old_block) {
                console.log("No block present in old block")
                await ethProcessHelper.setBlocks(config.REDISKEYS.ETH_BLOCKS, config.REDISKEYS.OLD_BLOCK_ETH, this.minBlockConfirmation, null)
            }
            if (!new_block) {
                console.log("No block present in new block")
                await ethProcessHelper.setBlocks(config.REDISKEYS.ETH_BLOCKS, config.REDISKEYS.NEW_BLOCK_ETH, this.minBlockConfirmation, null)
            }
            if (old_block && new_block) {
                const oldBlock: number = Number(old_block);
                const newBlock = Number(new_block);
                if (newBlock <= oldBlock) {
                    await ethProcessHelper.setBlocks(config.REDISKEYS.ETH_BLOCKS, config.REDISKEYS.NEW_BLOCK_ETH, this.minBlockConfirmation, newBlock)
                    return;
                } else {
                    const block_diff: number = newBlock - oldBlock;
                    if (block_diff > 50) {
                        await ethProcessHelper.diff_blocks_add_in_db(newBlock, oldBlock)
                        await ethProcessHelper.setKeyValuePair(config.REDISKEYS.ETH_BLOCKS, config.REDISKEYS.OLD_BLOCK_ETH, newBlock.toString())
                        await ethProcessHelper.setBlocks(config.REDISKEYS.ETH_BLOCKS, config.REDISKEYS.NEW_BLOCK_ETH, this.minBlockConfirmation, newBlock)
                    } else {
                        await this.readBlocks(oldBlock, newBlock)
                    }
                }
            } else {
                console.log("No old block and no new block")
                await globalHelper.saveErrorLogs('Eth_getBlocks', 'No old block no new block')
            }
        } catch (err: any) {
            console.error("Error in getBlocks", err)
            await globalHelper.saveErrorLogs('Eth_getBlocks', err.message)
        }
    }
    public async readBlocks(blockNumber: number, toBlockNumber: number) {
        try {
            const block_updated: number = blockNumber + 1;
            await ethProcessHelper.setKeyValuePair(config.REDISKEYS.ETH_BLOCKS, config.REDISKEYS.OLD_BLOCK_ETH, block_updated.toString());
            console.table({
                LATEST_BLOCK: blockNumber,
                BLOCK_DIFFERENCE: toBlockNumber - blockNumber,
                TO_BLOCK_NUMBER: toBlockNumber
            });
            await this.getTxFromBlock(blockNumber, KEYS.latestBlock);
        } catch (err: any) {
            console.error("Error in readBlocks>>.", (err as Error).message)
            await globalHelper.saveErrorLogs('ETH_readBlocks', err.message)
        }
    }
    public async getTxFromBlock(blockId: number, key: string) {
        try {
            console.error("In getTxFromBlock>>.", key, blockId)
            const blockInfo = await blockChainHelper.provider.getBlock(blockId, true);
            if (!blockInfo) {
                console.log("No block info");
                await ethOldBLockModel.create({
                    block_number: blockId,
                    coin_family: config.STATIC_COIN_FAMILY.ETH,
                    start_block: blockId,
                    end_block: blockId,
                    status: 0
                });
                return;
            }
            const transactions = blockInfo.prefetchedTransactions || [];
            if (transactions.length === 0) {
                console.log("No block info transactions");
                return;
            }


            const transactionPromises = transactions.map(async (tx) => {
                const data: blocksTrnx = {
                    block_number: blockId,
                    tx_id: tx.hash || '',
                    value: tx?.value.toString(),
                    fromAddress: tx.from || '',
                    toAddress: tx.to || '',
                    retry_count: 0,
                    key: null,
                    pair: null
                };

                data.key = (key === KEYS.latestBlock) ? 'latest' : 'behined & specific';
                const targetTopic = (key === KEYS.latestBlock) ? this.eth_block_tx_topic : this.eth_behind_block_tx_topic;

                try {
                    await kafkaHelper.produceMessage(targetTopic, data);
                } catch (err) {
                    console.error("Error while producing message for transaction", tx.hash, err);
                }
            });

            // Wait for all the transaction processing to complete
            await Promise.allSettled(transactionPromises);
        } catch (err: any) {
            console.error("Error in getTxFromBlock:", (err as Error).message);
            await globalHelper.saveErrorLogs('Eth_readBehindBlock', err.message)
        }
    }
    public async readBehindBlocks() {
        try {
            // REDIS BASED
            const data_exist: any = await ethProcessHelper.getKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_ETH)
            if (data_exist) {
                const data_exists = JSON.parse(data_exist);
                if (data_exists) {
                    console.log("This block exist in redis read behined")
                    let { id, start_block, end_block } = data_exists;
                    if (end_block > start_block) {
                        console.log("This block go for getTxFromBlock")
                        await this.getTxFromBlock(start_block, KEYS.behinedBlock);
                        start_block = start_block + 1;
                        const add_data = {
                            id: id,
                            start_block: start_block,
                            end_block: end_block,
                            diff: end_block - start_block
                        }
                        await ethProcessHelper.setKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_ETH, JSON.stringify(add_data))
                    } else {
                        console.log("id>>>>>>>>>>>", id)
                        await ethOldBLockModel.update({ status: 1 }, { where: { id: id } })
                        await ethProcessHelper.deleteKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_ETH)

                        await ethProcessHelper.behindBlocks()

                    }
                }
            } else {
                // DB BASED
                await ethProcessHelper.behindBlocks()
            }
        } catch (err: any) {
            console.error("Error in readBehindBlock>>", err)
            await globalHelper.saveErrorLogs('ETH_readBehindBlock', err.message)
        }
    }
    public async readSpecificBlock() {
        try {
            console.log("Entere into readSpecificBlock>>>")
            const specificBlocks: any = await ethProcessHelper.getKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_ETH)
            const specific_block: any = JSON.parse(specificBlocks);
            if (specific_block && Number(specific_block) > 0) {
                console.table({ SPECIFIC_BLOCK: specific_block })
                await this.getTxFromBlock(specific_block, KEYS.readSpecificBlock)
                await ethProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_ETH, '0')
            } else {
                console.log("Specific block is not present>>>>")
                await ethProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_ETH, '0')
            }
        } catch (error: any) {
            console.error("Error in readSpecificBlock>>>", error)
        }
    }


}

export const ethBlocksProcess = new EthBlockProcess();