import { Blockchain_Helper } from "../helpers/index";
import { config } from "../config/config";
import ethProcessHelper from "./process.helper";
import { KEYS } from "../enum";
import { blocks_trnx } from "../interfaces/global_helper";
// import rabbitMq from "../config/rabbitMq";
import { EthOldBLockModel } from "../models";
import { Op } from "sequelize";
import { global_helper } from "../helpers/global_helper";
import kafkaHelper from "../config/kafka.helper";
import { ethers } from "ethers";


class Eth_block_process {
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

    public async getBlocks() {
        try {
            /** Testing */
            // let { getBlockNumber } = Blockchain_Helper.Web3.eth;
            // let latestBlock = await getBlockNumber();
            // console.log("latestBlock>>", latestBlock)
            // return;
            let old_block: any = await ethProcessHelper.getKeyValuePair(config.REDISKEYS.ETH_BLOCKS, config.REDISKEYS.OLD_BLOCK_ETH)
            let new_block: any = await ethProcessHelper.getKeyValuePair(config.REDISKEYS.ETH_BLOCKS, config.REDISKEYS.NEW_BLOCK_ETH)
            if (!old_block) {
                console.log("No block present in old block")
                await ethProcessHelper.setBlocks(config.REDISKEYS.ETH_BLOCKS, config.REDISKEYS.OLD_BLOCK_ETH, this.minBlockConfirmation, null)
            }
            if (!new_block) {
                console.log("No block present in new block")
                await ethProcessHelper.setBlocks(config.REDISKEYS.ETH_BLOCKS, config.REDISKEYS.NEW_BLOCK_ETH, this.minBlockConfirmation, null)
            }
            if (old_block && new_block) {
                let oldBlock: number = Number(old_block);
                let newBlock = Number(new_block);
                if (newBlock <= oldBlock) {
                    await ethProcessHelper.setBlocks(config.REDISKEYS.ETH_BLOCKS, config.REDISKEYS.NEW_BLOCK_ETH, this.minBlockConfirmation, newBlock)
                } else {
                    let block_diff: number = newBlock - oldBlock;
                    if (block_diff > 50) {
                        await ethProcessHelper.diff_blocks_add_in_db(newBlock, oldBlock)
                        await ethProcessHelper.setKeyValuePair(config.REDISKEYS.ETH_BLOCKS, config.REDISKEYS.OLD_BLOCK_ETH, newBlock.toString())
                        await ethProcessHelper.setBlocks(config.REDISKEYS.ETH_BLOCKS, config.REDISKEYS.NEW_BLOCK_ETH, this.minBlockConfirmation, newBlock)
                    } else {
                        await this.readBlocks(oldBlock, newBlock)
                    }
                }
            } else {
                console.log(" no old block and no new block")
                await global_helper.save_error_logs('Eth_setBlocks', 'No old block no new block')
            }
        } catch (err: any) {
            console.error("Error in getBlocks", err)
            await global_helper.save_error_logs('Eth_getBlocks', err.message)
        }
    }
    public async readBlocks(blockNumber: number, toBlockNumber: number) {
        try {
            // console.log("blockNumber", blockNumber)
            let block_updated: number = blockNumber + 1;
            await ethProcessHelper.setKeyValuePair(config.REDISKEYS.ETH_BLOCKS, config.REDISKEYS.OLD_BLOCK_ETH, block_updated.toString());
            // console.log("blockNumber updated", block_updated)
            console.table({
                LATEST_BLOCK: blockNumber,
                BLOCK_DIFFERENCE: toBlockNumber - blockNumber,
                TO_BLOCK_NUMBER: toBlockNumber
            });
            await this.getTxFromBlock(blockNumber, KEYS.latestBlock);
        } catch (err: any) {
            console.error("Error in readBlocks>>.", (err as Error).message)
            await global_helper.save_error_logs('ETH_readBlocks', err.message)
        }
    }
    public async getTxFromBlock(blockId: number, key: string) {
        try {
            const blockInfo = await Blockchain_Helper.provider.getBlock(blockId, true);
            if (!blockInfo) {
                console.log("No block info");
                await EthOldBLockModel.create({
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
                // console.log('tx.hash >>', tx.hash, 'blockId >>>', blockId);
                const data: blocks_trnx = {
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
                    // If you were using RabbitMQ, you'd uncomment the appropriate line:
                    // const queue = (key === KEYS.latestBlock) ? config.TRANSACTIONS.eth_all_blocks : config.KAFKA_BLOCKS.eth_behined_block_topic;
                    // rabbitMq.send_tx_to_queue(queue, Buffer.from(JSON.stringify(data)));
                } catch (err) {
                    console.error("Error while producing message for transaction", tx.hash, err);
                }
            });

            // Wait for all the transaction processing to complete
            await Promise.allSettled(transactionPromises);
            // if (blockInfo) {
            //     if (blockInfo.transactions) {
            //         for await (let tx of blockInfo.transactions) {
            //             console.log('tx.hash >>', tx.hash, 'blockId >>>', blockId);
            //             let data: blocks_trnx = {
            //                 block_number: blockId,
            //                 tx_id: tx.hash ? tx.hash : '',
            //                 value: tx.value ? tx.value : '',
            //                 fromAddress: tx.from ? tx.from : '',
            //                 toAddress: tx.to ? tx.to : '',
            //                 retry_count: 0,
            //                 key: null,
            //                 pair: null
            //             }
            //             if (key == KEYS.latestBlock) {
            //                 data.key = 'latest';
            //                 // let queue_length: any = await rabbitMq.queue_length(config.TRANSACTIONS.eth_all_blocks)
            //                 // console.log("queue_length>>>", queue_length)
            //                 await kafkaHelper.produceMessage(this.eth_block_tx_topic, data);
            //                 //rabbitMq.send_tx_to_queue(config.TRANSACTIONS.eth_all_blocks, Buffer.from(JSON.stringify(data)))
            //             } else {
            //                 data.key = 'behined & specific'
            //                 await kafkaHelper.produceMessage(this.eth_behind_block_tx_topic, data);
            //                 //rabbitMq.send_tx_to_queue(config.KAFKA_BLOCKS.eth_behined_block_topic, Buffer.from(JSON.stringify(data)))
            //             }
            //         }
            //     } else {
            //         console.log("no block info trnasactions")
            //     }
            // } else {
            //     console.log("No blockinfo");
            //     await EthOldBLockModel.create({ block_number: blockId, coin_family: config.STATIC_COIN_FAMILY.BNB, start_block: blockId, end_block: blockId, status: 0 })
            // }
        } catch (err: any) {
            console.error("Error in getTxFromBlock:", (err as Error).message);
            await global_helper.save_error_logs('Eth_readBehindBlock', err.message)
        }
    }
    public async readBehindBlock() {
        try {
            // REDIS BASED
            let data_exist: any = await ethProcessHelper.getKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_ETH)
            if (data_exist) {
                let data_exists = JSON.parse(data_exist);
                if (data_exists) {
                    console.log("This block exist in redis read behined")
                    let { id, start_block, end_block } = data_exists;
                    if (end_block > start_block) {
                        console.log("This block go for getTxFromBlock")
                        await this.getTxFromBlock(start_block, KEYS.behinedBlock);
                        start_block = start_block + 1;
                        let add_data = {
                            id: id,
                            start_block: start_block,
                            end_block: end_block,
                            diff: end_block - start_block
                        }
                        await ethProcessHelper.setKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_ETH, JSON.stringify(add_data))
                    } else {
                        console.log("id>>>>>>>>>>>", id)
                        // await EthOldBLockModel.destroy({ where: { id: id } })
                        await EthOldBLockModel.update({ status: 1 }, { where: { id: id } })
                        await ethProcessHelper.deleteKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_ETH)

                        await ethProcessHelper.behindBlocks()

                    }
                }
            } else {
                // DB BASED
                await ethProcessHelper.behindBlocks()
            }

            // REDIS BASED
            // let data: any = await ethProcessHelper.getHashTable(config.READ_BEHINED_BLOCK_ETH)
            // console.log("data?>>??", data.length)
            // if (data.length > 0) {
            //     for (let i: number = 0; i < data.length; i++) {
            //         await eth_blocks_process.getTxFromBlock(Number(data[i]), KEYS.behinedBlock);
            //         await ethProcessHelper.deleteKeyValuePair(config.READ_BEHINED_BLOCK_ETH, data[i].toString())
            //     }
            // }
        } catch (err: any) {
            console.error("Error in readBehindBlock>>", err)
            await global_helper.save_error_logs('ETH_readBehindBlock', err.message)
        }
    }
    public async readSpecificBlock() {
        try {
            console.log("Entere into readSpecificBlock>>>")
            let specificBlocks: any = await ethProcessHelper.getKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_ETH)
            let specific_block: any = JSON.parse(specificBlocks);
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
    public async testingNode() {
        try {
            const provider = Blockchain_Helper.provider;

            // Get Block Number
            const latestBlock = await provider.getBlockNumber();
            console.log("Block number>>", latestBlock);

            const blockInfo = await provider.getBlock(latestBlock, true);
            console.log("blockInfo>>", blockInfo);

            if (blockInfo && blockInfo.transactions.length > 0) {
                let firstTxHash: string;
                const firstTx = blockInfo.transactions[0];

                if (typeof firstTx === 'string') {
                    firstTxHash = firstTx;
                } else {
                    // Assert the type to access the hash property safely
                    firstTxHash = (firstTx as ethers.TransactionResponse).hash;
                }
                // Get Block Receipt
                const txReceipt = await provider.getTransactionReceipt(firstTxHash);
                console.log("txreceipt>>", txReceipt);

                // Get more details (not reduced in ethers.js)
                const transaction = await provider.getTransaction(firstTxHash);
                console.log("transaction>>", transaction);
            } else {
                console.log("No transactions found in the block");
            }
        } catch (err: any) {
            console.error("Error in testingNode>>", err)
        }
    }
}

export const eth_blocks_process = new Eth_block_process();