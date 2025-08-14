import { blockChainHelper, utilityHelper } from "../helpers/index";
import { config } from "../config/config";
import ethProcessHelper from "./process.helper";
import { KEYS, TrnxTypeEnum } from "../enum";
import { blocksTrnx } from "../interfaces/globalHelper";
import { ethOldBLockModel } from "../models";
// import { Op } from "sequelize";
import { globalHelper } from "../helpers/global.helper";
import kafkaHelper from "../config/kafka.helper";
import { ethers, TransactionReceipt } from "ethers";
import { TxType } from "../types/ERC20Tokens.types";
import { BigNumber } from "bignumber.js";


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
            this.getBlockReceipts(blockNumber, KEYS.latestBlock);
            this.getTraceBlock(blockNumber, KEYS.latestBlock);
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
                        this.getBlockReceipts(start_block, KEYS.behinedBlock);
                        this.getTraceBlock(start_block, KEYS.behinedBlock);
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
                // await this.getTxFromBlock(specific_block, KEYS.readSpecificBlock)
                this.getBlockReceipts(specific_block, KEYS.readSpecificBlock);
                this.getTraceBlock(specific_block, KEYS.readSpecificBlock);
                await ethProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_ETH, '0')
            } else {
                console.log("Specific block is not present>>>>")
                await ethProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_ETH, '0')
            }
        } catch (error: any) {
            console.error("Error in readSpecificBlock>>>", error)
        }
    }

    public getTraceBlock = async (blockId: number, key: string) => {
        try {
            console.log("INSIDE getTraceBlock",blockId)
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
                    // Process main trace 
                    const bigInitValue: BigInt | string = (trace?.result?.value === "0x0" || trace?.result?.value === "0n" || trace?.result?.value == null) ? "0" : ethers.toBigInt(trace?.result?.value);
                    const value: BigNumber = new BigNumber(bigInitValue.toString());
                    // Identify COIN transactions (typically Ether transfers)
                    if (value.gt(0)) {
                        const bigInitGasUsed: BigInt | string = (trace?.result?.gasUsed === "0x0" || trace?.result?.gasUsed === "0n" || trace?.result?.gasUsed == null) ? "0" : ethers.toBigInt(trace?.result?.gasUsed);
                        const gasPrice: BigInt | string = (trace?.result?.gas === "0x0" || trace?.result?.gas === "0n" || trace?.result?.gas == null) ? "0" : ethers.toBigInt(trace?.result?.gas);
    
                        const txReceipt: any = {
                            tx_id: trace.txHash,
                            fromAddress: trace?.result?.from,
                            toAddress: trace?.result?.to,
                            value: value.toString(),
                            status: trace?.error ? false : true,
                            gasUsed: Number(bigInitGasUsed.toString()),
                            gasPrice: Number(gasPrice.toString()),
                            blockHash: trace?.blockHash || '',
                        }
    
                        await this.checkEthDeposit(txReceipt);
                    }    
                    // Process calls with non-zero value
                    else if (trace?.result?.calls && trace?.result?.calls.length > 0) {
                        for (const call of trace?.result?.calls) {
                            const callBigInitValue: BigInt | string = (call.value === "0x0" || call.value === "0n" || call.value == null) ? "0" : ethers.toBigInt(call.value);
                            const callValue: BigNumber = new BigNumber(callBigInitValue.toString());
                            
                            // Only process calls with non-zero value
                            if (callValue.gt(0)) {
                            const callBigInitGasUsed: BigInt | string = (call.gasUsed === "0x0" || call.gasUsed === "0n" || call.gasUsed == null) ? "0" : ethers.toBigInt(call.gasUsed);
                            const gasPrice: BigInt | string = (call.gas === "0x0" || call.gas === "0n" || call.gas == null) ? "0" : ethers.toBigInt(call.gas);

                            const callTxReceipt: any = {
                                tx_id: trace.txHash,
                                fromAddress: call.from,
                                toAddress: call.to,
                                value: callValue.toString(),
                                status: call?.error ? false : true,
                                gasUsed: Number(callBigInitGasUsed.toString()),
                                gasPrice: Number(gasPrice.toString()),
                                blockHash: trace.blockHash,
                            }

                            await this.checkEthDeposit(callTxReceipt);
                        }
                    }
                    }
                }
            } else {
                console.error("Error fetching data from QuickNode API.");
            }
        } catch (error) {
            console.log("🔥 ~ getTraceBlock error", (error as Error).message);
        }
    };
    
    public getBlockReceipts = async (blockId: number, key: string) => {
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
    
    private fetchBlockReceipts = async (blockId: number) => {
        const hexBlockValue = ethers.toBeHex(blockId.toString());
        const formattedBlockValue = hexBlockValue.replace(/^0x0+/g, '0x');
        return await blockChainHelper.provider.send("eth_getBlockReceipts", [formattedBlockValue]);
    };
    
    private processTxReceipt = async (txReceipt: any) => {
        // Only process successful transactions with logs
        if (!txReceipt?.status || !txReceipt?.logs?.length) {
            return;
        }
    
        const gasInfo =await this.extractGasInfo(txReceipt);
        const processedTokens = new Set<string>(); // Track processed NFT tokens per transaction
    
        for (const log of txReceipt.logs) {
            await this.processLog(log, gasInfo, processedTokens);
        }
    };
    
    private extractGasInfo = async (txReceipt: any) => {
        const gasUsed = txReceipt.gasUsed ? Number(txReceipt.gasUsed) : 0;
        const hexToNumberGasPrice = txReceipt?.gasPrice ? Number(txReceipt.gasPrice) : 0;
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
            await this.processTokenTransfer(log, gasInfo, contractInfo);
        }
        else{ 
            const nftInfo = await ethProcessHelper.checkIfOurNFTContract(log.address)
            if (this.isNFTTransfer(log, nftInfo)) {
            await this.processNFTTransfer(log, gasInfo, nftInfo, processedTokens);
            return;
        }}
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
    
    private calculateTokenAmount = async (data: string, decimals: number): Promise<number> => {
        const decimalPrecision = decimals.toString().length - 1;
        return await utilityHelper.bigNumberSafeMath(data, '/', Math.pow(10, decimalPrecision));
    };
        public async checkEthDeposit(data: blocksTrnx) {
            try {
                /** for check the eth deposit */
                if (
                    data.value !== '0' &&
                    data.toAddress
                ) {
                    const gasUsed = data.gasUsed ? data.gasUsed : 0;
                    const effectiveGasPrice: number = Number(data.gasPrice) || 0;
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
                }else{
                    console.log("else block checkEthDeposit")
                }
            } catch (error: any) {
                console.error("💥 ~ ~ check_eth_deposit error", (error as Error).message);
                await globalHelper.saveErrorLogs('ETH_check_deposit', error.message)
    
            }
        }
    public async checkAddressInDB(txData: TxType) {
        try {
            console.log("INSIDE checkAddressInDB")
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

        public async addTxToQueue(data: TxType, queueName: string) {
            try {
                await kafkaHelper.produceMessage(queueName, data)
            } catch (err: any) {
                console.error("addTxToQueue", (err as Error).message);
                await globalHelper.saveErrorLogs('ETH_checkAddressInDB', err.message)
    
            }
        };

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
}

export const ethBlocksProcess = new EthBlockProcess();