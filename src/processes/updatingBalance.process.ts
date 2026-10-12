import { config } from "../config";
import kafkaHelper from "../config/kafka.helper";
import { walletHelper } from "../helpers";
import { balanceUpdateQueue } from "../interfaces/globalHelper";
import { walletModel } from "../models";
import  axios from "axios";
import { GasPrice } from "../models/tables/model.gas_price";

// Fix: Import STATIC_COIN_FAMILY from the correct config location
import { config as appConfig } from "../config/config";
const { ETH } = appConfig.STATIC_COIN_FAMILY;

class UpdateBalanceController {


    public eth_pending_balance_update_topic = config.config.KAFKA_BALANCE.ETH_PENDING_BALANCE_UPDATE_TOPIC;

    public updateWalletBalance = async () => {
        let consumer: any = null;
        let isConnected = false;

        const connectConsumer = async () => {
            try {

                if (consumer) {
                    await consumer.disconnect();
                }

                consumer = await kafkaHelper.kafka.consumer({
                    groupId: config.config.KAFKA_BALANCE.ETH_PENDING_BALANCE_UPDATE_GROUP_ID,
                    // Add session timeout for better error handling
                    sessionTimeout: 30000,
                    // Add heartbeat interval
                    heartbeatInterval: 3000,
                });

                await consumer.subscribe({
                    topic: this.eth_pending_balance_update_topic,
                    fromBeginning: true, // ✅ Changed to false to prevent reprocessing
                });

                await consumer.run({
                    // ✅ Add proper autoCommit configuration
                    autoCommit: true,
                    autoCommitInterval: 5000,
                    autoCommitThreshold: 100,

                    eachMessage: async ({ topic, partition, message }: { topic: any; partition: any; message: any }) => {
                        try {
                            console.log("🚀 ~ updateWalletBalance topic >>>>", topic);
                            console.log("🚀 ~ updateWalletBalance message.value >>>>", message.value.toString());
                            await this.updateBalance(JSON.parse(message.value.toString()))

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
                        console.log(`🔄 Attempting to connect consumer updateWalletBalance...`);
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


    public updateBalance = async (data: balanceUpdateQueue) => {
        try {
            if (data.queue_count < 10) {
                const balance: number | null = await walletHelper.getBalance(data.wallet_address, data.coin_data);
                if (balance !== null) {
                    await walletModel.update(
                        { balance: Number(balance) },
                        { where: { wallet_address: data.wallet_address, coin_id: data.coin_data.coin_id } })
                } else {
                    if (data.queue_count <= 5) {
                        await kafkaHelper.produceMessage(this.eth_pending_balance_update_topic, {
                            coin_data: data.coin_data,
                            wallet_address: data.wallet_address,
                            queue_count: data.queue_count + 1
                        });
                    }
                }

                console.log("balance:::", balance);
            }
            else {
                console.log(`updating balance attempts are more than 10 `);
            }

        } catch (err: any) {
            console.error("Error in updateBalance", err)
        }
    }

    
       //TODO : temprory cron for ETH gas  price  update 
       public updateGasPrice = async () => {
        try {
          let url: string = `${appConfig.ETH_GAS_FEE_URL}${appConfig.ETH_GAS_FEE_API_KEY}`;
          await axios.get(url).then(async (responce) => {
            if (responce.status == 200) {
              let SafeGasPrice = parseFloat(responce.data.result.SafeGasPrice);
              let ProposeGasPrice = parseFloat(
                responce.data.result.ProposeGasPrice
              );
              let FastGasPrice = parseFloat(responce.data.result.FastGasPrice);
    
              // Round to 6 decimal places if necessary
              if (SafeGasPrice.toString().split(".")[1]?.length > 6) {
                SafeGasPrice = parseFloat(SafeGasPrice.toFixed(6));
              }
    
              // Round to 6 decimal places if necessary
              if (ProposeGasPrice.toString().split(".")[1]?.length > 6) {
                ProposeGasPrice = parseFloat(ProposeGasPrice.toFixed(6));
              }
    
              // Round to 6 decimal places if necessary
              if (FastGasPrice.toString().split(".")[1]?.length > 6) {
                FastGasPrice = parseFloat(FastGasPrice.toFixed(6));
              }
    
              if(FastGasPrice>0){
                await GasPrice.GasPriceWrite.update(
                  {
                    safe_gas_price: SafeGasPrice,
                    propose_gas_price: ProposeGasPrice,
                    fast_gas_price: FastGasPrice,
                  },
                  {
                    where: { coin_family: ETH },
                  }
                );
              }
            }
          });
          return;
        } catch (err: any) {
          let message: any = (err as Error).message;
          console.error("🔥 ~ ~ updateGasPrice error", message);
          return;
        }
      };


}


export const updateBalanceProcess = new UpdateBalanceController();