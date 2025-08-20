import database from "../config/db";
import Sequelize, { QueryTypes } from "sequelize";
import { CoinInterface, coinsModel, ethOldBLockModel, WalletInterface } from "../models";
import redisClient from "../config/redis";
import { promisify } from "util";
import { config } from "../config/config";
import { TokenStandard } from "../enum";
import { globalHelper } from "../helpers/global.helper";
import { blockChainHelper } from "../helpers";
import { Op } from "sequelize";

class EthProcessHelper {
  public CoinsInterface: CoinInterface[];
  public WalletAddresses: WalletInterface[];

  constructor() {
    this.fetchERC20Tokens();
    this.fetchERC721Tokens();
    this.fetchWalletAddress();
  }
  public async getKeyValuePair(keyName: string, field: string) {
    try {
      const getAsync = promisify(redisClient.HGET).bind(redisClient);
      const value = await getAsync(keyName, field.toUpperCase() || "");
      return value;
    } catch (err: any) {
      console.error('getKeyValuePair >>>>', err);
      return null;
    }
  }

  public async getMultipleKeyValuePairs(keyName: string, fields: string[]) {
    try {
      const upperFields = fields.map(f => f.toUpperCase());
      return new Promise((resolve, reject) => {
        redisClient.hmget(keyName, ...upperFields, (err, results) => {
          if (err) reject(err);
          else resolve(results);
        });
      });
    } catch (err: any) {
      console.error('getMultipleKeyValuePairs >>>>', err);
      return null;
    }
  }

  public async setBlocks(keyName: string, filed: string, minBlockConfirmation: number, newBlock: number | null) {
    try {
      let latestBlock = await blockChainHelper.provider.getBlockNumber();
      latestBlock -= minBlockConfirmation;

      const shouldUpdate = !newBlock || newBlock < latestBlock;

      if (shouldUpdate) {
        await ethProcessHelper.setKeyValuePair(keyName, filed, latestBlock.toString());
      } else {
        console.log("No update required for block number");
      }
    } catch (err: any) {
      console.error("Error in setBlocks:", err);
      await globalHelper.saveErrorLogs('ETH_setBlocks', err.message);
    }
  }

  // public async setBlocks(field: string, key: string, minBlockConfirmation: number, newBlock: number | null) {
  //   try {
  //     let latestBlock = await blockChainHelper.provider.getBlockNumber();
  //     /** Minus Buffer Block From Latest Block */
  //     latestBlock = latestBlock - minBlockConfirmation;
  //     if (newBlock) {
  //       if (newBlock < latestBlock) {
  //         await ethProcessHelper.setKeyValuePair(field, key, latestBlock.toString())
  //       } else {
  //         console.log("No updation of blocks")
  //       }
  //     } else {
  //       await ethProcessHelper.setKeyValuePair(field, key, latestBlock.toString())
  //     }
  //   } catch (err: any) {
  //     console.error("Error in  getBlocks", err)
  //     await globalHelper.saveErrorLogs('ETH_setBlocks', err.message)
  //   }
  // }

  public async setKeyValuePair(keyName: string, field: string, value: string) {
    try {
      redisClient.hset(keyName, field.toUpperCase(), value);
    } catch (err: any) {
      console.error('setKeyValuePair >>>', err);
    }
  }

  public async diff_blocks_add_in_db(current_block: number, old_block: number) {
    try {
      // const value_exist: any = await this.getKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.NODE_ALERT_ETH)
      // if (value_exist) {
      //   const setvalue: number = Number(value_exist) + 1;
      //   if (Number(value_exist) <= 100) {
      //     await this.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.NODE_ALERT_ETH, setvalue.toString())
      //     if (Number(value_exist) == 0 || Number(value_exist) == 5 || Number(value_exist) == 10 || Number(value_exist) == 20 || Number(value_exist) == 50 || Number(value_exist) == 75 || Number(value_exist) == 100) {
      //       let email_body = `<p>Ethereum Node Issue ${Number(value_exist)}</p><p>Current block running = ${current_block}.</p><p>Old Block = ${old_block}.</p>`;
      //       await global_helper.sendMail(config.NODE_ISSUE_ALERT_EMAIL, email_body, `Ethereum Node Issue ${Number(value_exist)}`);
      //     } else {
      //       console.log("no email")
      //     }
      //   } else {
      //     console.log("Number(value_exist) > 100")
      //   }
      // } else {
      //   await this.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.NODE_ALERT_ETH, '0')
      // }
      const dataToInsert = { block_number: old_block, coin_family: config.STATIC_COIN_FAMILY.ETH, start_block: old_block, end_block: current_block, status: 0 }
      console.log("dataToInsert>>>>", dataToInsert)
      await ethOldBLockModel.create(dataToInsert)
      return true;
    } catch (err: any) {
      console.error("💥 ~ Error in diff_blocks_add_in_redis.", err)
      await globalHelper.saveErrorLogs('ETH_diff_blocks_add_in_db', err.message)
      return false;
    }
  }

  public async getHashTable(keyName: string) {
    try {
      const getAsync: any = promisify(redisClient.hvals).bind(redisClient)
      const value: any = await getAsync(keyName)
      return value;
    } catch (err: any) {
      console.error("Error in getHashTable>>>", err)
      return null;
    }
  }
  public async deleteKeyValuePair(keyName: string, field: string) {
    try {
      redisClient.hdel(keyName, field.toUpperCase(), function (err) {
        if (err) {
          throw err;
        }
        return true;
      })
    } catch (err: any) {
      console.error("Error in deleteKeyValuePair>>>", err)
      return false;
    }
  }

  public fetchERC20Tokens = async () => {
    try {
      const tokens: any = await coinsModel.findAll({
        attributes: ["coin_id", "coin_symbol", "coin_family", "is_token", "token_type", "decimals", "token_address", "for_swap", "added_by"],
        where: {
          coin_status: 1,
          coin_family: config.STATIC_COIN_FAMILY.ETH,
          [Op.or]: [{ token_type: TokenStandard.ERC20 }, { token_type: null }]
        },
        raw: true,
        // logging: true
      });
      // console.log('`${config.TOKEN_TYPE_ETH}`', `${config.TOKEN_TYPE_ETH}`)
      for await (const token of tokens) {
        const findTokenData = await this.getKeyValuePair(
          `${config.TOKEN_TYPE_ETH}`,
          token.token_address == null ? 'NativeCoin'.toUpperCase() : token.token_address.toUpperCase(),
        );
        if (!findTokenData) {
          this.setKeyValuePair(
            `${config.TOKEN_TYPE_ETH}`,
            token.token_address == null ? 'NativeCoin'.toUpperCase() : token.token_address.toUpperCase(),
            // token.token_address.toUpperCase(),
            JSON.stringify(token)
          );
        }
      }

      // redisClient.set(config.TOKEN_TYPE_ETH || "", JSON.stringify(tokens));
    } catch (err: any) {
      console.error('fetchERC20Tokens >>>>>', err);
      await globalHelper.saveErrorLogs('ETH_fetchERC20Tokens', err.message)
    }
  }

  public fetchWalletAddress = async () => {
    try {
      const getaddressQuery = `SELECT ws.user_id,ws.wallet_address as address, ws.wallet_name FROM wallets ws FORCE INDEX (coin_family) JOIN coins cs ON ws.coin_id = cs.coin_id WHERE cs.coin_family = ${config.STATIC_COIN_FAMILY.ETH} GROUP BY ws.wallet_address`;
      const addresses: any = await database.db_write.query(
        getaddressQuery,
        { type: QueryTypes.SELECT }
      );
      for await (let wallet of addresses) {
        let findAddress = await this.getKeyValuePair(
          `${config.ETH_WALLET_ADDRESS}`,
          wallet.address.toUpperCase()
        );
        if (!findAddress) {
          this.setKeyValuePair(
            `${config.ETH_WALLET_ADDRESS}`,
            wallet.address.toUpperCase(),
            JSON.stringify(wallet)
          );
        }
      }
    } catch (error) {
      console.error('💥 ~ fetchWalletAddress error', error);
    }
  };

  public checkIfContract = async (address: string) => {
    try {
      const token_data: any = await this.getKeyValuePair(config.TOKEN_TYPE_ETH, address);
      // if (token_data) {
      //   console.log("token_data>>>>>>>>", token_data)
      // }
      return JSON.parse(token_data);
    } catch (err: any) {
      console.error('check_our_wallet_address >>>>>', err);
      return null;
    }
  };

  // public checkIfContract = async (address: string) => {
  //   try {
  //     const getAsync = promisify(redisClient.get).bind(redisClient);
  //     const value = await getAsync(config.TOKEN_TYPE_ETH || "");

  //     const erc20Tokens: CoinInterface[] = value ? JSON.parse(value) : [];
  //     const token = erc20Tokens.find(
  //       (el: any) => Number(el.is_token) === Number(BooleanEnum.true) && el.token_address?.toUpperCase() === address?.toUpperCase()
  //     );
  //     if (token) return token;
  //     return null;
  //   } catch (error: any) {
  //     console.error('checkIfContract >>>', error);
  //     return null;
  //   }
  // };

  public fetchERC721Tokens = async () => {
    try {
      const getTokenQuery = `SELECT coin_id,coin_symbol,coin_family,is_token, token_type,decimals,token_address,for_swap, added_by FROM coins WHERE coin_status=1 AND coin_family=${config.STATIC_COIN_FAMILY.ETH} AND token_type!='${TokenStandard.ERC20}' AND token_type IS NOT NULL`;

      const tokens: CoinInterface[] = await database.db_read.query(getTokenQuery, {
        type: Sequelize.QueryTypes.SELECT,
      });
      if (tokens.length > 0) {
        for await (let token of tokens) {
          if (token.token_address) {
            this.setKeyValuePair(
              `${config.ETH_NFT}`,
              token.token_address.toUpperCase(),
              JSON.stringify(token)
            );
          }
        }
      }
    } catch (error: any) {
      console.error('fetchERC721Tokens >>>>', error);
    }
  };

  public checkIfOurNFTContract = async (address: string) => {
    try {
      const token = await this.getKeyValuePair(`${config.ETH_NFT}`, address);
      if (token) {
        return JSON.parse(token);
      } else {
        return {}
      }
    } catch (error) {
      console.error(`💥 ~ checkIfContract error`, error);
    }
  };

  public check_our_wallet_address = async (address: string) => {
    try {
      const wallet_data: any = await this.getKeyValuePair(config.ETH_WALLET_ADDRESS, address);
      if (wallet_data) {
        console.log("wallet_Data>>>>>>>>", wallet_data)
      }
      return JSON.parse(wallet_data);
    } catch (error: any) {
      console.error('check_our_wallet_address >>>>>', error);
      return null;
    }
  };


  // public NativeCoinByCoinFamily = async (
  //   coin_family: CoinFamilyEnum
  // ) => {
  //   try {
  //     const getAsync = promisify(redisClient.get).bind(redisClient);
  //     const value = await getAsync(config.TOKEN_TYPE_ETH || "");

  //     const erc20Tokens: CoinInterface[] = value ? JSON.parse(value) : [];
  //     return erc20Tokens.find(
  //       (el: CoinInterface) => Number(el.is_token) === Number(BooleanEnum.false) && el.coin_family === coin_family
  //     );

  //   } catch (error: any) {
  //     console.error(`NativeCoinByCoinFamily >>>`, error);
  //     return 0;
  //   }
  // }

  // public CoinByCoinId = async (
  //   coin_id: number
  // ) => {
  //   try {
  //     const getAsync = promisify(redisClient.get).bind(redisClient);
  //     const value = await getAsync(config.TOKEN_TYPE_ETH || "");

  //     const erc20Tokens: CoinInterface[] = value ? JSON.parse(value) : [];
  //     return erc20Tokens.find(
  //       (el: CoinInterface) => el.coin_id === coin_id
  //     );

  //   } catch (error: any) {
  //     console.error(`CoinByCoinId >>>`, error);
  //     return null;
  //   }
  // }
  public behindBlocks = async () => {
    try {
      const behindBlocks: any = await ethOldBLockModel.findOne({
        attributes: ['id', 'block_number', 'start_block', 'end_block'],
        where: { status: 0 },
        raw: true,
        order: [['id', 'ASC']]
      })
      if (behindBlocks) {
        const add_data = {
          id: behindBlocks.id,
          start_block: behindBlocks.start_block,
          end_block: behindBlocks.end_block,
          diff: behindBlocks.end_block - behindBlocks.start_block
        }
        await this.setKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_ETH, JSON.stringify(add_data))
      } else {
        console.log("you have not any behind block yet")
      }
    } catch (err: any) {
      console.error(`behindBlocks >>>`, err);
      return null;
    }
  }


  private async addCoinToTable(
    tokenAddress: string,
    symbol: string,
    name: string,
    coinFamily: number,
    tokenType: string,
    decimals: any,
    currentUTCDate: any,
    userId: number
  ) {
    try {
      //will  be  updated  later 
      let swapSupported: number = 0;

      let image: any = null;
      let cmcId: number | null = null;
      let graphSource: string | null = null;
      let graphPair: string | null = null;

      if (tokenAddress && symbol) {

        const cmcPayload: any = {
          symbol: symbol,
          coinFamily: coinFamily,
          tokenAddress: tokenAddress,
        }
        /// remove this added through cron job
        const cmc_data: any = await globalHelper.getTokenImageFromCmcBySymbol(cmcPayload);

        console.log("this is the cmc_data >>>", cmc_data);
        cmcId = cmc_data ? cmc_data.id : 0;
        image = cmc_data ? cmc_data.image : null;
        // remove this added through the cron job
        const tradingViewData = await globalHelper.getTradingSource(symbol);
        console.log("tradingViewData >>>", tradingViewData);
        if (tradingViewData.status) {
          graphSource = tradingViewData.graphSource;
          graphPair = tradingViewData.graphPair;
        }
      }
      console.log("coinZone>>>", image, "token_type>>", image);

      if (cmcId == null) {
        cmcId = 0;
      }

      const coinData: any = {
        coin_name: name,
        coin_symbol: symbol.toUpperCase(),
        coin_image: image,
        cmc_id: cmcId,
        coin_family: coinFamily,
        coin_status: 1,
        is_token: 1,
        is_on_cmc: cmcId > 0 ? 1 : 0,
        for_swap: swapSupported,
        added_by: "user",
        token_type: tokenType.toUpperCase(),
        decimals: Math.pow(10, parseInt(decimals)),
        token_address: tokenAddress,
        created_at: currentUTCDate,
        updated_at: currentUTCDate,
        graph_pair: graphPair,
        graph_source: graphSource,
        is_on_trading_view: graphSource ? 1 : 0,
      };
      const response: any = await globalHelper.coinCreate(coinData);
      console.log("🚀 ~ processHelper ~ response:", response)

      coinData.coin_id = response.coin_id;
      const redisCoinData = {
        coin_id: coinData.coin_id,
        is_token: coinData.is_token,
        token_address: coinData.token_address,
        coin_family: coinData.coin_family,
        decimals: coinData.decimals,
        token_type: coinData.token_type,
        coin_symbol: coinData.coin_symbol,
        added_by: coinData.added_by,
        for_swap: coinData.for_swap
      }

      //update  token in redis hash 
      await globalHelper.updateTokenInRedisHash(redisCoinData);
      //insert token in custom token table 
      await globalHelper.customTokenCreate({
        coin_id: coinData.coin_id,
        user_id: userId,
      });
      return redisCoinData;
    } catch (err: any) {
      console.error("Error in add_coin_to_table>>", err);
      await globalHelper.saveErrorLogs("add_coin_to_table", err.message);
      return null;
    }
  }




  public async insertTokenAddressInDB(tokenAddress: string, tokenDetail: any, nativeWalletData: any) {
    try {
      console.log("data in insertTokenAddressInDB", nativeWalletData)
      const walletName = nativeWalletData.wallet_name;
      const walletAddress = nativeWalletData.address;
      const userId = nativeWalletData.user_id;
      const coinFamily = config.STATIC_COIN_FAMILY.ETH;
      const tokenType = TokenStandard.ERC20;
      const decimals = tokenDetail.decimals;
      const symbol = tokenDetail.symbol;
      const name = tokenDetail.name;
      const currentUTCDate: string = new Date()
        .toISOString()
        .replace(/T/, " ")
        .replace(/\..+/, "");

      const coinData: any = await this.addCoinToTable(
        tokenAddress,
        symbol,
        name,
        coinFamily,
        tokenType,
        decimals,
        currentUTCDate,
        userId
      );


      if (
        coinFamily == config.STATIC_COIN_FAMILY.ETH
      ) {
        const redisTokenData: any = {
          coin_family: coinFamily,
          token_address: tokenAddress,
        };

        //Todo : Need  to confirm from barjinder sir  if we need to produce message to kafka or not 
        await kafka_helper.produceMessage(
          config.MORALIS_TRADINGVIEW_TOPIC,
          redisTokenData
        );
      }


      //create wallet for coin  :: need to confirm from barjinder sir  if  needed  or not 
      //no need this please remove this. this work done by checkCoinInWallet this function
      const message: string = await globalHelper.createWalletForCoin(
        walletAddress,
        coinData.coin_id,
        walletName,
        userId,
        coinFamily,
        currentUTCDate,
        tokenAddress,
        1,
        tokenType
      );

      return coinData;

    } catch (error) {
      console.error(`insertTokenAddressInDB >>>`, error);
      return null;
    }




  }


}
const ethProcessHelper = new EthProcessHelper();
export default ethProcessHelper;