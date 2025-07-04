import { config } from "../config/config";
import kafkaHelper from "../config/kafka.helper";
import { blockChainHelper, utilityHelper } from "../helpers";
import { blocksTrnx } from "../interfaces/globalHelper";
import ethProcessHelper from "./process.helper";
import { TxType } from "../types/ERC20Tokens.types";
import { TrnxTypeEnum } from "../enum";
import { globalHelper } from "../helpers/global.helper";
import { ethers, TransactionReceipt } from "ethers";

class ProcessedBlockTransactions {
    public approval: string | null;
    public swap: string | null;
    public transfer: string | null;

    public eth_block_tx_topic = config.KAFKA_BLOCKS.ETH_BLOCK_TX_TOPIC;
    public eth_block_tx_group_id = config.KAFKA_BLOCKS.ETH_BLOCK_TX_GROUP_ID;
    public eth_behind_block_tx_topic = config.KAFKA_BLOCKS.ETH_BEHIND_BLOCK_TX_TOPIC;
    public eth_behind_block_tx_group_id = config.KAFKA_BLOCKS.ETH_BEHIND_BLOCK_TX_GROUP_ID;
    public deposit_withdraw_topic = config.KAFKA_TRANSACTIONS.DEPOSIT_WITHDRAW_TOPIC;

    constructor() {
        this.approval = ethers.id('Approval(address,address,uint256)');
        this.swap = ethers.id('Swap(address,uint256,uint256,uint256,uint256,address)');
        this.transfer = ethers.id('Transfer(address,address,uint256)');

    }

    public startBlockTxQueue = async () => {
        let consumer: any = null;
        let isConnected = false;

        const connectConsumer = async () => {
            try {

                if (consumer) {
                    await consumer.disconnect();
                }

                consumer = await kafkaHelper.kafka.consumer({
                    groupId: this.eth_block_tx_group_id,
                    // Add session timeout for better error handling
                    sessionTimeout: 30000,
                    // Add heartbeat interval
                    heartbeatInterval: 3000,
                });

                await consumer.subscribe({
                    topic: this.eth_block_tx_topic,
                    fromBeginning: true, // ✅ Changed to false to prevent reprocessing
                });

                await consumer.run({
                    // ✅ Add proper autoCommit configuration
                    autoCommit: true,
                    autoCommitInterval: 5000,
                    autoCommitThreshold: 100,

                    eachMessage: async ({ topic, partition, message }: { topic: any; partition: any; message: any }) => {
                        try {
                            console.log("🚀 ~ startBlockTxQueue topic >>>>", topic);
                            console.log("🚀 ~ startBlockTxQueue message.value >>>>", message.value.toString());
                            await this.readTrnx(JSON.parse(message.value.toString()))

                        } catch (error) {
                            console.error(`❌ Error processing message:`, error);
                        }
                    },
                });
                isConnected = true;
            } catch (error) {
                console.error(`❌ Failed to connect consumer:`, error);
                isConnected = false;
                throw error;
            }
        };

        // ✅ Add reconnection logic
        const startWithReconnection = async () => {
            while (true) {
                try {
                    if (!isConnected) {
                        console.log(`🔄 Attempting to connect consumer startBlockTxQueue...`);
                        await connectConsumer();
                    }

                    // Keep the consumer alive
                    await new Promise(resolve => setTimeout(resolve, 30000));

                } catch (error) {
                    console.error(`❌ Consumer connection error:`, error);
                    isConnected = false;

                    // Wait before retrying
                    console.log(`⏳ Waiting 10 seconds before reconnecting...`);
                    await new Promise(resolve => setTimeout(resolve, 10000));
                }
            }
        };

        // Start the consumer with reconnection
        await startWithReconnection();
    };

    // public startBlockTxQueue = async () => {
    //     const consumer = await kafkaHelper.kafka.consumer({ groupId: this.eth_block_tx_group_id });
    //     await consumer.subscribe({ topic: this.eth_block_tx_topic, fromBeginning: true });

    //     await consumer.run({
    //         eachMessage: ({ topic, partition, message }: { topic: any, partition: any, message: any }) => {
    //             this.readTrnx(JSON.parse(message.value.toString()))
    //         }
    //     }).catch((err: Error) => {
    //         console.log("🔥 ~ startBlockTxQueue consumer error", err);
    //     });
    // };

    public startBehindBlockTxQueue = async () => {
        let consumer: any = null;
        let isConnected = false;

        const connectConsumer = async () => {
            try {

                if (consumer) {
                    await consumer.disconnect();
                }

                consumer = await kafkaHelper.kafka.consumer({
                    groupId: this.eth_behind_block_tx_group_id,
                    // Add session timeout for better error handling
                    sessionTimeout: 30000,
                    // Add heartbeat interval
                    heartbeatInterval: 3000,
                });

                await consumer.subscribe({
                    topic: this.eth_behind_block_tx_topic,
                    fromBeginning: false, // ✅ Changed to false to prevent reprocessing
                });

                await consumer.run({
                    // ✅ Add proper autoCommit configuration
                    autoCommit: true,
                    autoCommitInterval: 5000,
                    autoCommitThreshold: 100,

                    eachMessage: async ({ topic, partition, message }: { topic: any; partition: any; message: any }) => {
                        try {
                            console.log("🚀 ~ startBehindBlockTxQueue topic >>>>", topic);
                            console.log("🚀 ~ startBehindBlockTxQueue message.value >>>>", message.value.toString());
                            await this.readTrnx(JSON.parse(message.value.toString()))

                        } catch (error) {
                            console.error(`❌ Error processing message:`, error);
                        }
                    },
                });
                isConnected = true;
            } catch (error) {
                console.error(`❌ Failed to connect consumer:`, error);
                isConnected = false;
                throw error;
            }
        };

        // ✅ Add reconnection logic
        const startWithReconnection = async () => {
            console.log('isConnected >>>', isConnected);
            while (true) {
                try {
                    if (!isConnected) {
                        console.log(`🔄 Attempting to connect consumer startBehindBlockTxQueue...`);
                        await connectConsumer();
                    }

                    // Keep the consumer alive
                    await new Promise(resolve => setTimeout(resolve, 30000));

                } catch (error) {
                    console.error(`❌ Consumer connection error:`, error);
                    isConnected = false;

                    // Wait before retrying
                    console.log(`⏳ Waiting 10 seconds before reconnecting...`);
                    await new Promise(resolve => setTimeout(resolve, 10000));
                }
            }
        };

        // Start the consumer with reconnection
        await startWithReconnection();
    };

    // public startBehindBlockTxQueue = async () => {
    //     const consumer = await kafkaHelper.kafka.consumer({ groupId: this.eth_behind_block_tx_group_id });
    //     await consumer.subscribe({ topic: this.eth_behind_block_tx_topic, fromBeginning: true });

    //     await consumer.run({
    //         eachMessage: ({ topic, partition, message }: { topic: any, partition: any, message: any }) => {
    //             this.readTrnx(JSON.parse(message.value.toString()))
    //         }
    //     }).catch((err: Error) => {
    //         console.log("🔥 ~ startBehindBlockTxQueue consumer error", err);
    //     });
    // };
    public async readTrnx(data: blocksTrnx) {
        try {
            const txReceipt: TransactionReceipt | null = await blockChainHelper.provider.getTransactionReceipt(data.tx_id);
            if (txReceipt == null && Number(data.retry_count) < 6) {
                data.retry_count = Number(data.retry_count) + 1;
                await kafkaHelper.produceMessage(this.eth_behind_block_tx_topic, data)
                return false;
            }

            if (txReceipt?.status == 0) {
                console.log("(txReceipt.status === false && txReceipt !== null")
            } else if (txReceipt) {
                await this.buildTx(txReceipt, data)
            }
        } catch (error: any) {
            console.error("Error in read_trnx>>>", error)
            if (Number(data.retry_count) < 3) {
                data.retry_count = Number(data.retry_count) + 1;
                console.log("Adding transaction in queue>>>", data.retry_count, "transaction id>>>", data)
                await kafkaHelper.produceMessage(this.eth_behind_block_tx_topic, data)
            }
        }
    }
    public async buildTx(txReceipt: TransactionReceipt, data: blocksTrnx) {
        try {
            if (txReceipt.status !== 0 && txReceipt.logs.length > 0) {
                let nft_tokens_address: any = [];
                for await (const log of txReceipt.logs) {
                    const isToken: any = await ethProcessHelper.checkIfContract(log.address);
                    if (isToken && log?.topics && log?.topics[2] !== undefined && log?.topics[0] !== undefined) {
                        if (log.topics[0] == this.approval) {
                            continue;
                        } else if (log.topics[0] == this.transfer) {
                            const gasUsed = txReceipt.gasUsed ? Number(txReceipt.gasUsed) : 0;
                            const hexToNumberGasPrice: any = txReceipt?.gasPrice ? Number(txReceipt.gasPrice) : 0;
                            const txFee = await utilityHelper.bigNumberSafeMath(gasUsed, '*', hexToNumberGasPrice);

                            const gasInEth = await blockChainHelper.convertWeiToEth(txFee);

                            const decimalPrecision = isToken?.decimals?.toString().length - 1;
                            let finalAmount = await utilityHelper.bigNumberSafeMath(log.data, '/', Math.pow(10, decimalPrecision))
                            finalAmount = await utilityHelper.exponentialToDecimal(finalAmount)
                            let toAddress: string = '0x' + log.topics[2].substring(26, log.topics[2].length);
                            /* in case of swap trnx */
                            let fromAddress: string = '0x' + log.topics[1].substring(26, log.topics[1].length);
                            const txData: TxType = {
                                txId: log.transactionHash,
                                fromAddress: fromAddress,
                                toAddress: toAddress,
                                token: isToken,
                                amount: finalAmount,
                                blockId: log.blockNumber,
                                isNFTToken: false,
                                txType: null,
                                trnx_fee: Number(gasInEth)
                            };
                            await this.checkAddressInDB(txData);

                        }
                    } else {
                        const isNFTToken = await ethProcessHelper.checkIfOurNFTContract(log.address);
                        if (Object.keys(isNFTToken)?.length > 0 && log.topics[1] != undefined && log.topics[2] != undefined) {
                            const transferData: any = await this.getTransferEventDetail(
                                isNFTToken.token_address,
                                log?.blockNumber
                            );
                            if (nft_tokens_address.indexOf(isNFTToken.token_address) == -1 && transferData?.length > 0) {
                                const gasUsed = txReceipt.gasUsed ? Number(txReceipt.gasUsed) : 0;
                                const hexToNumberGasPrice: any = txReceipt?.gasPrice ? Number(txReceipt.gasPrice) : 0;
                                const txFee = await utilityHelper.bigNumberSafeMath(gasUsed, '*', hexToNumberGasPrice);

                                const gasInEth = await blockChainHelper.convertWeiToEth(txFee);

                                nft_tokens_address.push(isNFTToken.token_address);
                                const fromAddress = transferData[0]?.args[0];
                                const toAddress = transferData[0]?.args[1];
                                const tokenId = ethers.toNumber(transferData[0]?.args[2]);
                                const txData: TxType = {
                                    txId: transferData[0].transactionHash,
                                    fromAddress: fromAddress,
                                    toAddress: toAddress,
                                    token: isNFTToken,
                                    amount: tokenId,
                                    blockId: log?.blockNumber,
                                    isNFTToken: true,
                                    txType: null,
                                    trnx_fee: Number(gasInEth)
                                };
                                await this.checkAddressInDB(txData);
                            } else {
                                return "Not get transfer data.";
                            }
                        }
                    }
                    /** swap transaction then also check eth */
                    if (log?.topics && log?.topics[0] == this.swap) {
                        await this.checkEthDeposit(data, txReceipt);
                    } else if (isToken == null && log?.topics[2] !== undefined && log?.topics[0] == this.transfer) {
                        await this.checkEthDeposit(data, txReceipt)
                    }
                }
            } else {
                await this.checkEthDeposit(data, txReceipt);
            }
        } catch (error: any) {
            console.error("buildTx error >>>", (error as Error).message);
            await globalHelper.saveErrorLogs('Eth_buildTx', error.message)
        }
    };
    public async checkEthDeposit(data: blocksTrnx, txReceipt: TransactionReceipt) {
        try {
            /** for check the eth deposit */
            if (
                data.value !== '0' &&
                data.toAddress &&
                data.block_number
            ) {
                const gasUsed = txReceipt.gasUsed ? txReceipt.gasUsed : 0;
                const effectiveGasPrice: number = Number(txReceipt.gasPrice) || 0;
                const txFee = await utilityHelper.bigNumberSafeMath(gasUsed, '*', effectiveGasPrice);
                const gasInEth = await blockChainHelper.convertWeiToEth(txFee);

                const ethValue = await blockChainHelper.convertWeiToEth((data.value ? data.value : 0));
                const amount = parseFloat(ethValue.toString());

                const txData: TxType = {
                    txId: data.tx_id,
                    fromAddress: data.fromAddress.toLowerCase(),
                    toAddress: data.toAddress.toLowerCase(),
                    token: null,
                    amount,
                    blockId: data?.block_number,
                    isNFTToken: false,
                    txType: null,
                    trnx_fee: Number(gasInEth)
                };
                await this.checkAddressInDB(txData);
            }
        } catch (error: any) {
            console.error("💥 ~ ~ check_eth_deposit error", (error as Error).message);
            await globalHelper.saveErrorLogs('ETH_check_deposit', error.message)

        }
    }
    public async checkAddressInDB(txData: TxType) {
        try {
            // let ourTrnx: any = null;
            const addresses = [txData.fromAddress, txData.toAddress].filter(Boolean);

            if (addresses.length > 0) {
                const results = await ethProcessHelper.getMultipleKeyValuePairs(`${config.ETH_WALLET_ADDRESS}`, addresses);

                if (results) {
                    const [withdrawTx, depositTx] = results as string[];
                    if (depositTx) {
                        // console.log('Deposit results >>>', depositTx);
                        txData.txType = TrnxTypeEnum.DEPOSIT;
                    } else if (withdrawTx) {
                        // console.log('Withdraw results >>>', withdrawTx);
                        txData.txType = TrnxTypeEnum.WITHDRAW;
                    }
                    // ourTrnx = txData;
                }
                // return;
            }
            // const withdraw = await ethProcessHelper.check_our_wallet_address(txData.fromAddress);
            // if (withdraw) {
            //     console.log("Found the user in FROM (WITHDRAW) tx 😸");
            //     txData.txType = TrnxTypeEnum.WITHDRAW;
            //     ourTrnx = txData;
            // }
            // const deposit = await ethProcessHelper.check_our_wallet_address(txData.toAddress);
            // if (deposit) {
            //     console.log("Found the user in to (DEPOSIT) tx 😸");
            //     txData.txType = TrnxTypeEnum.DEPOSIT;
            //     ourTrnx = txData;
            // }
            if (txData.txType !== null) {
                // console.log('ourTrnx.txType >>>', typeof txData.txType);
                await this.addTxToQueue(
                    txData,
                    this.deposit_withdraw_topic || ""
                );
            }
        } catch (err: any) {
            console.error("Error in checkAddressInDB>>", err)
        }
    }

    public getTransferEventDetail = async (
        tokenAddress: string,
        blockId: number
    ) => {
        try {
            const contract = new ethers.Contract(
                tokenAddress,
                config.ERC721ABI,
                blockChainHelper.provider
            );

            const filter = contract.filters.Transfer();
            const events = await contract.queryFilter(
                filter,
                blockId,
                blockId
            );

            return events;
        } catch (error) {
            console.log("🔥 ~ getTransferEventDetail error", (error as Error).message);
            return [];
        }
    };

    public async addTxToQueue(data: TxType, queueName: string) {
        try {
            await kafkaHelper.produceMessage(queueName, data)
        } catch (err: any) {
            console.error("addTxToQueue", (err as Error).message);
            await globalHelper.saveErrorLogs('ETH_checkAddressInDB', err.message)

        }
    };

}
export const processedBlockTransactions = new ProcessedBlockTransactions()
