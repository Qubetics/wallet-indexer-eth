import kafkaHelper from "./config/kafka.helper";
// import { ethWithdrawDepositProcess, ethPendingWithdrawalProcess, ethTxStatusUpdateProcess, ethBlocksProcess, processedBlockTransactions, updateBalanceProcess } from "./processes/index.process"
import { ethWithdrawDepositProcess, ethPendingWithdrawalProcess, ethTxStatusUpdateProcess, ethBlocksProcess, updateBalanceProcess } from "./processes/index.process"
import schedule from "node-schedule";
class ETHStartProcess {
  constructor() {
    this.connectKafka();
    this.readBlockProcess();
    this.readSpecificBlock();
    // this.consumeBlockTxQueue();
    // this.consumeBehindBlockTxQueue();
    this.consumeWithdrawDepositTxsQueue();
    this.internalTxsInQueue();
    this.consumeInternalTxQueue();
    this.consumeUpdateBalanceQueue();
  }

  public async connectKafka() {
    await kafkaHelper.connect();
  }

  public readBlockProcess() {
    schedule.scheduleJob("*/8 * * * * *", async function () {
      await ethBlocksProcess.readCurrentBlocks();
      await ethBlocksProcess.readBehindBlocks()
    });
  }
  // public testing_node() {
  //   schedule.scheduleJob("*/2 * * * * *", async function () {
  //     await eth_blocks_process.testingNode();
  //   });
  // }
  public readSpecificBlock() {
    schedule.scheduleJob('*/1 * * * *', async function () {
      await ethBlocksProcess.readSpecificBlock();
    })
  }
  // public consumeBlockTxQueue() {
  //   setTimeout(async () => {
  //     await processedBlockTransactions.startBlockTxQueue();

  //   }, 10000);
  // }
  // public consumeBehindBlockTxQueue() {
  //   setTimeout(async () => {
  //     await processedBlockTransactions.startBehindBlockTxQueue();
  //   }, 10000);
  // }
  public async consumeWithdrawDepositTxsQueue() {
    setTimeout(async () => {
      await ethWithdrawDepositProcess.startDepositQueue();
    }, 10000);
  }
  public internalTxsInQueue() {
    schedule.scheduleJob("*/10 * * * * *", async function () {
      await ethPendingWithdrawalProcess.getTransactionFromDB();
    });
  }
  public async consumeInternalTxQueue() {
    setTimeout(async () => {
      await ethTxStatusUpdateProcess.startTxStatusUpdateQueue();
    }, 10000);
  }
  public async consumeUpdateBalanceQueue() {
    setTimeout(async () => {
      await updateBalanceProcess.updateWalletBalance();
    }, 10000);
  }
}

new ETHStartProcess();
