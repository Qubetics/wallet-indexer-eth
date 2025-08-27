import { blockChainHelper, utilityHelper } from "../helpers/index";
import { config } from "../config/config";
import ethProcessHelper from "./process.helper";
import { KEYS, TrnxTypeEnum } from "../enum";
import { blocksTrnx } from "../interfaces/globalHelper";
import { ethOldBLockModel } from "../models";
import { globalHelper } from "../helpers/global.helper";
import kafkaHelper from "../config/kafka.helper";
import { ethers, TransactionReceipt } from "ethers";
import { TxType } from "../types/ERC20Tokens.types";
import { BigNumber } from "bignumber.js";
import { ethEther } from "../helpers/ether_eth_helpers";
import { TokenData } from "../interfaces/ether.interface";


class EthBlockProcess {
    public minBlockConfirmation: number;
    public swap: string | null;
    public approval: string | null;
    public transfer_by_partition: string | null;
    public approval_by_partition: string | null;
    public processed_dividend_tracker: string | null;
    public dividend_withdrawn: string | null;
    public deposit: string | null;
    public eth_block_tx_topic = config.KAFKA_BLOCKS.ETH_BLOCK_TX_TOPIC;
    public eth_behind_block_tx_topic = config.KAFKA_BLOCKS.ETH_BEHIND_BLOCK_TX_TOPIC;
    public deposit_withdraw_topic = config.KAFKA_TRANSACTIONS.DEPOSIT_WITHDRAW_TOPIC;
    public trace_block_eth_deposit_topic = "trace_block_eth_deposit_topic";

    constructor() {
        this.minBlockConfirmation = Number(config.MIN_BLOCK_CONFORMATIONS);
        this.swap = ethers.id('Swap(address,uint256,uint256,uint256,uint256,address)');
        this.approval = ethers.id('Approval(address,address,uint256)');
        this.transfer_by_partition = ethers.id('TransferByPartition(bytes32,address,address,address,uint256,bytes,bytes)');
        this.approval_by_partition = ethers.id('ApprovalByPartition(bytes32,address,address,uint256)');
        this.processed_dividend_tracker = ethers.id('ProcessedDividendTracker(uint256,uint256,uint256,bool,uint256,address)');
        this.dividend_withdrawn = ethers.id('DividendWithdrawn(address,uint256)');
        this.deposit = ethers.id('Deposit(address,uint256)');
    }

    public async readCurrentBlocks() {
        try {
            const results = await ethProcessHelper.getMultipleKeyValuePairs(config.REDISKEYS.ETH_BLOCKS, [config.REDISKEYS.OLD_BLOCK_ETH, config.REDISKEYS.NEW_BLOCK_ETH]);
            console.log("🚀 ~ EthBlockProcess ~ readCurrentBlocks ~ results:", results)

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
            // await this.getTxFromBlock(blockNumber, KEYS.latestBlock);
            this.getBlockReceipts(blockNumber);
            this.getTraceBlock(blockNumber);
        } catch (err: any) {
            console.error("Error in readBlocks>>.", (err as Error).message)
            await globalHelper.saveErrorLogs('ETH_readBlocks', err.message)
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
                        // await this.getTxFromBlock(start_block, KEYS.behinedBlock);
                        this.getBlockReceipts(start_block);
                        this.getTraceBlock(start_block);
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
            console.log("Enter into readSpecificBlock>>>")
            const specificBlocks: any = await ethProcessHelper.getKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_ETH)
            const specific_block: any = JSON.parse(specificBlocks);
            if (specific_block && Number(specific_block) > 0) {
                console.table({ SPECIFIC_BLOCK: specific_block })
                // await this.getTxFromBlock(specific_block, KEYS.readSpecificBlock)
                this.getBlockReceipts(specific_block);
                this.getTraceBlock(specific_block);
                await ethProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_ETH, '0')
            } else {
                console.log("Specific block is not present>>>>")
                await ethProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_ETH, '0')
            }
        } catch (error: any) {
            console.error("Error in readSpecificBlock>>>", error)
        }
    }
    
    public getTraceBlock = async (blockId: number) => {
        try {
            console.log("INSIDE getTraceBlock", blockId)
            const hexBlockValue = ethers.toBeHex(blockId.toString());
            const formattedBlockValue = hexBlockValue.replace(/^0x0+/g, '0x');
            const response = await blockChainHelper.provider.send("debug_traceBlockByNumber", [formattedBlockValue,
                {
                    "tracer": "callTracer"
                }]);

            if (response?.length > 0) {
                const traces = response;
                // Loop through the traces and identify transaction types
                for (const trace of traces) {
                    if (trace?.error) {
                        console.log("(trace?.error", trace?.error);
                        return false;
                    }

                    // if(trace.txHash == "0x9e124bf89e92a56d015b69e6b8a05865c3c1f7e20940d810c916feeb6c457839"){


                    // Process main trace 
                    const bigInitValue: BigInt | string = (trace?.result?.value === "0x0" || trace?.result?.value === "0n" || trace?.result?.value == null) ? "0" : ethers.toBigInt(trace?.result?.value);
                    const value: BigNumber = new BigNumber(bigInitValue.toString());
                    // Identify COIN transactions (typically Ether transfers)
                    if (trace?.result?.type == 'CALL' && value.gt(0) && trace?.result?.calls?.length > 0) {

                        // const bigInitGasUsed: BigInt | string = (trace?.result?.gasUsed === "0x0" || trace?.result?.gasUsed === "0n" || trace?.result?.gasUsed == null) ? "0" : ethers.toBigInt(trace?.result?.gasUsed);
                        // const gasPrice: BigInt | string = (trace?.result?.gas === "0x0" || trace?.result?.gas === "0n" || trace?.result?.gas == null) ? "0" : ethers.toBigInt(trace?.result?.gas);
    
                        const txReceipt: blocksTrnx = {
                            txId: trace.txHash,
                            fromAddress: trace?.result?.from,
                            toAddress: trace?.result?.to,
                            value: value.toString(),
                            gasUsed: null,
                            gasPrice: null,
                            blockNumber: blockId
                        }

                        console.log("inside  if 11111", txReceipt);
                        //here  will add the txReceipt to the kafka topic
                        this.addTraceTxToQueue(txReceipt, this.trace_block_eth_deposit_topic);

                        // await this.checkEthDeposit(txReceipt);
                    }
                    // Process calls with non-zero value
                    else if (trace?.result?.calls && trace?.result?.calls?.length > 0) {
                        
                        for (const call of trace?.result?.calls) {
                            if (call?.error) {
                                console.log("(call?.error", call?.error);
                                return false;
                            }
                            const callBigInitValue: BigInt | string = (call.value === "0x0" || call.value === "0n" || call.value == null) ? "0" : ethers.toBigInt(call.value);
                            const callValue: BigNumber = new BigNumber(callBigInitValue.toString());

                            // Only process calls with non-zero value
                            if (call?.type == 'CALL' && callValue.gt(0)) {
                                /** if internal calls present */
                                if (call?.calls?.length > 0) {
                                    for (const call2 of call?.calls) {
                                        if (call2?.type == 'CALL' && call2?.value && Number(call2?.value) > 0) {
                                            const callTxReceipt: blocksTrnx = {
                                                txId: trace.txHash,
                                                fromAddress: call2.from,
                                                toAddress: call2.to,
                                                value: Number(call2.value).toString(),
                                                gasUsed: null,
                                                gasPrice: null,
                                                blockNumber: blockId
                                            }

                                            console.log("inside  if 22222", callTxReceipt);
                                            // await this.checkEthDeposit(callTxReceipt)
                                            this.addTraceTxToQueue(callTxReceipt, this.trace_block_eth_deposit_topic);
                                        }
                                    }
                                } else {
                                    // const callBigInitGasUsed: BigInt | string = (call.gasUsed === "0x0" || call.gasUsed === "0n" || call.gasUsed == null) ? "0" : ethers.toBigInt(call.gasUsed);
                                    // const gasPrice: BigInt | string = (call.gas === "0x0" || call.gas === "0n" || call.gas == null) ? "0" : ethers.toBigInt(call.gas);

                                    const callTxReceipt: blocksTrnx = {
                                        txId: trace.txHash,
                                        fromAddress: call.from,
                                        toAddress: call.to,
                                        value: callValue.toString(),
                                        gasUsed: null,
                                        gasPrice: null,
                                        blockNumber: blockId
                                    }

                                    console.log("inside  if 33333", callTxReceipt);

                                    // await this.checkEthDeposit(callTxReceipt);
                                    this.addTraceTxToQueue(callTxReceipt, this.trace_block_eth_deposit_topic);
                                }
                            } else if (call?.type == 'DELEGATECALL' && callValue.gt(0)) {
                                if (call?.calls?.length > 0) {
                                    for (const call2 of call?.calls) {
                                        if (call2?.type == 'CALL' && call2?.value && Number(call2?.value) > 0) {
                                            const callTxReceipt: blocksTrnx = {
                                                txId: trace.txHash,
                                                fromAddress: call2.from,
                                                toAddress: call2.to,
                                                value: Number(call2.value).toString(),
                                                gasUsed: null,
                                                gasPrice: null,
                                                blockNumber: blockId
                                            }

                                            console.log("inside  if 44444", callTxReceipt);
                                            // await this.checkEthDeposit(callTxReceipt)
                                            this.addTraceTxToQueue(callTxReceipt, this.trace_block_eth_deposit_topic);
                                        }
                                    }
                                } else {
                                    const callTxReceipt: blocksTrnx = {
                                        txId: trace.txHash,
                                        fromAddress: call.from,
                                        toAddress: call.to,
                                        value: callValue.toString(),
                                        gasUsed: null,
                                        gasPrice: null,
                                        blockNumber: blockId
                                    }

                                    console.log("inside  if 55555", callTxReceipt);

                                    // await this.checkEthDeposit(callTxReceipt);
                                    this.addTraceTxToQueue(callTxReceipt, this.trace_block_eth_deposit_topic);
                                }
                            }
                        }
                    }

                }
            } else {
                console.error("Error fetching data from QuickNode API.");
                this.getTxFromBlock(blockId);

            }
        } catch (error) {
            console.log("🔥 ~ getTraceBlock error", (error as Error).message);
            this.getTxFromBlock(blockId);
        }
    };
    
    public getBlockReceipts = async (blockId: number) => {
        try {
            console.log("INSIDE getBlockReceipts")
            // Get block receipts
            const blockReceipts = await this.fetchBlockReceipts(blockId);
            if (!blockReceipts) {
                console.error(`💥 ~ No transactions found in block ${blockId}`);
                return false;
            }

            // Process each transaction receipt
            for (const txReceipt of blockReceipts) {
                await this.processTxReceipt(txReceipt);
            }

            return true;
        } catch (error) {
            console.log("🔥 ~ getBlockReceipts error", (error as Error).message);
            return false;
        }
    };

    public async getTxFromBlock(blockId: number) {
        try {
            console.error("In getTxFromBlock>>.", blockId)
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
                const amount: BigNumber = new BigNumber(tx?.value.toString());
                if (amount.gt(0)) {
                    const txReceipt: TransactionReceipt | null = await blockChainHelper.provider.getTransactionReceipt(tx?.hash);

                    if (txReceipt?.status == 0) {
                        console.log("(txReceipt.status === false && txReceipt !== null");
                        return false;
                    }
                    const gasUsed = txReceipt?.gasUsed ? txReceipt.gasUsed : 0;
                    const gasPrice: number = Number(txReceipt?.gasPrice) || 0;
                    const blockTransaction: blocksTrnx = {
                        txId: tx.hash || '',
                        value: amount?.toString(),
                        fromAddress: tx.from || '',
                        toAddress: tx.to || '',
                        blockNumber: blockId,
                        gasUsed: Number(gasUsed.toString()),
                        gasPrice: gasPrice,
                    }
                    await this.checkEthDeposit(blockTransaction);
                }
            });

            // Wait for all the transaction processing to complete
            await Promise.allSettled(transactionPromises);
        } catch (err: any) {
            console.error("Error in getTxFromBlock:", (err as Error).message);
            await globalHelper.saveErrorLogs('Eth_readBehindBlock', err.message)
        }
    }

    private fetchBlockReceipts = async (blockId: number) => {
        const hexBlockValue = ethers.toBeHex(blockId.toString());
        const formattedBlockValue = hexBlockValue.replace(/^0x0+/g, '0x');
        return await blockChainHelper.provider.send("eth_getBlockReceipts", [formattedBlockValue]);
    };

    private processTxReceipt = async (txReceipt: TransactionReceipt) => {
        // Only process successful transactions with logs
        if (!txReceipt?.status || !txReceipt?.logs?.length) {
            return;
        }

        const gasInfo = await this.extractGasInfo(txReceipt);
        const processedTokens = new Set<string>(); // Track processed NFT tokens per transaction

        for (const log of txReceipt.logs) {
            await this.processLog(log, gasInfo, processedTokens);
        }
    };

    private extractGasInfo = async (txReceipt: any) => {
        const gasUsed = txReceipt.gasUsed ? Number(txReceipt.gasUsed) : 0;
        const hexToNumberGasPrice = txReceipt?.effectiveGasPrice ? Number(txReceipt.effectiveGasPrice) : 0;
        const txFee = await utilityHelper.bigNumberSafeMath(gasUsed, '*', hexToNumberGasPrice);
        const gasInEth = await blockChainHelper.convertWeiToEth(txFee);

        return {
            gasUsed: gasUsed,
            gasPrice: hexToNumberGasPrice,
            trnx_fee: Number(gasInEth)
        };
    };

    private processLog = async (log: any, gasInfo: any, processedTokens: Set<string>) => {



        const contractInfo = await ethProcessHelper.checkIfContract(log.address);
        if (this.isTokenTransfer(log, contractInfo)) {
            //here  we have contractInfo.token_address
            await this.processTokenTransfer(log, gasInfo, contractInfo);
        }
        else {
            const nftInfo = await ethProcessHelper.checkIfOurNFTContract(log.address)
            if (this.isNFTTransfer(log, nftInfo)) {
                await this.processNFTTransfer(log, gasInfo, nftInfo, processedTokens);
                return;
            } else if (this.isTokenTransferNotExist(log)) {
                //here  we have the from and to address 
                const toAddress = '0x' + log.topics[2]?.substring(26);
                const addresses = [toAddress].filter(Boolean);

                if (addresses.length > 0) {
                    const results = await ethProcessHelper.getMultipleKeyValuePairs(`${config.ETH_WALLET_ADDRESS}`, addresses);
                    if (results) {
                        const [depositTx] = results as string[];
                        if (depositTx) {
                            const nativeWalletData: number = depositTx ? JSON.parse(depositTx) : null;
                            if (nativeWalletData) {
                                console.log("contractInfo not exist in db 💥💥 inserting into db", log.address)
                                const tokenDetail: TokenData | null = await ethEther.tokenDetails(log.address);
                                if (tokenDetail) {
                                    const contractInfo = await ethProcessHelper.insertTokenAddressInDB(log.address, tokenDetail, nativeWalletData);
                                    if (contractInfo) {
                                        await this.processNotExistTokenTransfer(log, gasInfo, contractInfo, TrnxTypeEnum.DEPOSIT)
                                    } else {
                                        return;
                                    }

                                }
                            } else {
                                return;
                            }

                        } else {
                            return;
                        }
                    }
                }
            }
        }
    };

    private isNFTTransfer = (log: any, nftInfo: any): boolean => {
        return nftInfo && Object.keys(nftInfo)?.length > 0 &&
            log.topics[1] !== undefined &&
            log.topics[2] !== undefined;
    };

    private isTokenTransfer = (log: any, contractInfo: any): boolean => {
        return contractInfo && Object.keys(contractInfo)?.length > 0 &&
            log.topics[2] !== undefined &&
            !this.isExcludedTopic(log.topics[0]);
    };

    private isTokenTransferNotExist = (log: any): boolean => {
        return log &&
            log.topics[1] !== undefined &&
            log.topics[2] !== undefined &&
            !this.isExcludedTopic(log.topics[0]);
    };

    private isExcludedTopic = (topic: string): boolean => {
        const excludedTopics = [
            this.approval,
            this.approval_by_partition,
            this.transfer_by_partition,
            this.processed_dividend_tracker,
            this.dividend_withdrawn
        ];
        return excludedTopics.includes(topic);
    };

    private processNFTTransfer = async (
        log: any,
        gasInfo: any,
        nftInfo: any,
        processedTokens: Set<string>
    ) => {
        // Avoid processing the same NFT token multiple times in one transaction
        if (processedTokens.has(nftInfo.token_address)) {
            return;
        }

        const transferData: any = await this.getTransferEventDetail(
            nftInfo.token_address,
            log.blockNumber
        );

        if (!transferData?.length) {
            console.log("Not get transfer data.");
            return;
        }

        processedTokens.add(nftInfo.token_address);

        const fromAddress = transferData[0]?.args[0];
        const toAddress = transferData[0]?.args[1];
        const tokenId = ethers.toNumber(transferData[0]?.args[2]);

        const txData: TxType = {
            txId: transferData[0].transactionHash,
            fromAddress: fromAddress,
            toAddress: toAddress,
            token: nftInfo,
            amount: tokenId,
            blockId: log.blockNumber,
            isNFTToken: true,
            txType: null,
            trnx_fee: gasInfo.trnx_fee
        };

        await this.checkAddressInDB(txData);
    };

    private processTokenTransfer = async (log: any, gasInfo: any, contractInfo: any) => {
        const amount = await this.calculateTokenAmount(log.data, contractInfo.decimals);

        if (amount <= 0) {
            return;
        }

        const txData: TxType = {
            txId: log.transactionHash,
            fromAddress: '0x' + log.topics[1]?.substring(26),
            toAddress: '0x' + log.topics[2]?.substring(26),
            token: contractInfo,
            amount: amount,
            blockId: log.blockNumber,
            isNFTToken: false,
            txType: null,
            trnx_fee: gasInfo.trnx_fee
        };

        await this.checkAddressInDB(txData);
    };

    private processNotExistTokenTransfer = async (log: any, gasInfo: any, contractInfo: any, txType: TrnxTypeEnum.DEPOSIT) => {
        const amount = await this.calculateTokenAmount(log.data, contractInfo.decimals);
        if (amount <= 0) {
            return;
        }

        const txData: TxType = {
            txId: log.transactionHash,
            fromAddress: '0x' + log.topics[1]?.substring(26),
            toAddress: '0x' + log.topics[2]?.substring(26),
            token: contractInfo,
            amount: amount,
            blockId: log.blockNumber,
            isNFTToken: false,
            txType: txType,
            trnx_fee: gasInfo.trnx_fee
        };

        if (txData.txType !== null) {
            await this.addTxToQueue(
                txData,
                this.deposit_withdraw_topic || ""
            );
        }
    };

    private calculateTokenAmount = async (data: string, decimals: number): Promise<number> => {
        const decimalPrecision = decimals.toString().length - 1;
        return await utilityHelper.bigNumberSafeMath(data, '/', Math.pow(10, decimalPrecision));
    };

    public async checkEthDeposit(data: blocksTrnx) {
        try {
            /** for check the eth deposit */
            if (
                data.toAddress
            ) {

                // console.log("inside checkEthDeposit this is data.toAddress ", data.toAddress)

                const addresses = [data.fromAddress, data.toAddress].filter(Boolean);
                
                let txType = null;
                if (addresses.length > 0) {
                    const results = await ethProcessHelper.getMultipleKeyValuePairs(`${config.ETH_WALLET_ADDRESS}`, addresses);


                    if (results) {
                        const [withdrawTx, depositTx] = results as string[];
                        if (depositTx) {
                            txType = TrnxTypeEnum.DEPOSIT;
                        } else if (withdrawTx) {
                            txType = TrnxTypeEnum.WITHDRAW;
                        }
                    }
                }
                if (txType !== null) {

                    if (data.gasUsed == null && data.gasPrice == null) {
                        const txReceipt: TransactionReceipt | null = await blockChainHelper.provider.getTransactionReceipt(data?.txId);
                        const gasUsed: number = Number(txReceipt?.gasUsed) || 0;
                        data.gasUsed = gasUsed;
                        data.gasPrice = Number(txReceipt?.gasPrice) || 0;
                    }

                    const gasUsed = data.gasUsed ? data.gasUsed : 0;
                    const effectiveGasPrice: number = Number(data.gasPrice) || 0;
                    const txFee = await utilityHelper.bigNumberSafeMath(gasUsed, '*', effectiveGasPrice);
                    const gasInEth = await blockChainHelper.convertWeiToEth(txFee);

                    const ethValue = await blockChainHelper.convertWeiToEth((data.value ? data.value : 0));
                    const amount = parseFloat(ethValue.toString());

                    const txData: TxType = {
                        txId: data.txId,
                        fromAddress: data.fromAddress.toLowerCase(),
                        toAddress: data.toAddress.toLowerCase(),
                        token: null,
                        amount,
                        blockId: data?.blockNumber,
                        isNFTToken: false,
                        txType: txType,
                        trnx_fee: Number(gasInEth)
                    };

                    await this.addTxToQueue(
                        txData,
                        this.deposit_withdraw_topic || ""
                    );
                }
            } else {
                console.log("else block checkEthDeposit")
            }
        } catch (error: any) {
            console.error("💥 ~ ~ check_eth_deposit error", (error as Error).message);
            await globalHelper.saveErrorLogs('ETH_check_deposit', error.message)

        }
    }

    private async checkAddressInDB(txData: TxType) {
        try {
            // let ourTrnx: any = null;
            const addresses = [txData.fromAddress, txData.toAddress].filter(Boolean);

            if (addresses.length > 0) {
                const results = await ethProcessHelper.getMultipleKeyValuePairs(`${config.ETH_WALLET_ADDRESS}`, addresses);
                if (results) {
                    const [withdrawTx, depositTx] = results as string[];
                    if (depositTx) {
                        txData.txType = TrnxTypeEnum.DEPOSIT;
                    } else if (withdrawTx) {
                        txData.txType = TrnxTypeEnum.WITHDRAW;
                    }
                }
            }

            if (txData.txType !== null) {
                await this.addTxToQueue(
                    txData,
                    this.deposit_withdraw_topic || ""
                );
            }
        } catch (err: any) {
            console.error("Error in checkAddressInDB>>", err)
        }
    }

    private async addTxToQueue(data: TxType, queueName: string) {
        try {
            await kafkaHelper.produceMessage(queueName, data)
        } catch (err: any) {
            console.error("addTxToQueue", (err as Error).message);
            await globalHelper.saveErrorLogs('ETH_checkAddressInDB', err.message)

        }
    };

    private async addTraceTxToQueue(data: blocksTrnx, queueName: string) {
        try {
            await kafkaHelper.produceMessage(queueName, data)
        } catch (err: any) {
            console.error("addTxToQueue", (err as Error).message);
            await globalHelper.saveErrorLogs('ETH_checkAddressInDB', err.message)

        }
    };

    private getTransferEventDetail = async (
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
}

export const ethBlocksProcess = new EthBlockProcess();