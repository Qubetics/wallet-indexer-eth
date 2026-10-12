import database from "../config/db";
import Sequelize, { QueryTypes } from "sequelize";
import { CoinInterface, CoinsModel, EthOldBLockModel, WalletInterface } from "../models";
import redisClient from "../config/redis";
import { promisify } from "util";
import { config } from "../config/config";
import { TokenStandard } from "../enum";
import { global_helper } from "../helpers/global_helper";
import { Blockchain_Helper } from "../helpers";
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
  public async setBlocks(field: string, key: string, minBlockConfirmation: number, newBlock: number | null) {
    try {
      let latestBlock = await Blockchain_Helper.provider.getBlockNumber();
      latestBlock = latestBlock - minBlockConfirmation;
      if (newBlock) {
        if (newBlock < latestBlock) {
          await ethProcessHelper.setKeyValuePair(field, key, latestBlock.toString())
        } else {
          console.log("No updation of blocks")
        }
      } else {
        await ethProcessHelper.setKeyValuePair(field, key, latestBlock.toString())
      }
    } catch (err: any) {
      console.error("Error in  getBlocks", err)
      await global_helper.save_error_logs('ETH_setBlocks', err.message)
    }
  }
  public async setKeyValuePair(keyName: string, field: string, value: string) {
    try {
      redisClient.hset(keyName, field.toUpperCase(), value);
    } catch (err: any) {
      console.error('setKeyValuePair >>>', err);
    }
  }
  public async diff_blocks_add_in_db(current_block: number, old_block: number) {
    try {
      let value_exist: any = await this.getKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.NODE_ALERT_ETH)
      if (value_exist) {
        let setvalue: number = Number(value_exist) + 1;
        if (Number(value_exist) <= 100) {
          await this.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.NODE_ALERT_ETH, setvalue.toString())
          if (Number(value_exist) == 0 || Number(value_exist) == 5 || Number(value_exist) == 10 || Number(value_exist) == 20 || Number(value_exist) == 50 || Number(value_exist) == 75 || Number(value_exist) == 100) {
            let email_body = `<p>Ethereum Node Issue ${Number(value_exist)}</p><p>Current block running = ${current_block}.</p><p>Old Block = ${old_block}.</p>`;
            await global_helper.send_mail(config.NODE_ISSUE_ALERT_EMAIL, email_body, `Ethereum Node Issue ${Number(value_exist)}`);
          } else {
            console.log("no email")
          }
        } else {
          console.log("Number(value_exist) > 100")
        }
      } else {
        await this.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.NODE_ALERT_ETH, '0')
      }
      let dataToInsert = { block_number: old_block, coin_family: config.STATIC_COIN_FAMILY.ETH, start_block: old_block, end_block: current_block, status: 0 }
      console.log("dataToInsert>>>>", dataToInsert)
      await EthOldBLockModel.create(dataToInsert)
      return true;
    } catch (err: any) {
      console.error("💥 ~ Error in diff_blocks_add_in_redis.", err)
      await global_helper.save_error_logs('ETH_diff_blocks_add_in_db', err.message)
      return false;
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
      let tokens: any = await CoinsModel.findAll({
        attributes: ["coin_id", "coin_symbol", "coin_family", "is_token", "token_type", "decimals", "token_address", "for_swap", "added_by"],
        where: {
          coin_status: 1,
          coin_family: config.STATIC_COIN_FAMILY.ETH,
          [Op.or]: [{ token_type: TokenStandard.ERC20 }, { token_type: null }]
        },
        raw: true,
        logging: true
      });
      console.log('`${config.TOKEN_TYPE_ETH}`', `${config.TOKEN_TYPE_ETH}`)
      for await (let token of tokens) {
        let findTokenData = await this.getKeyValuePair(
          `${config.TOKEN_TYPE_ETH}`,
          token.token_address == null ? 'NativeCoin'.toUpperCase() : token.token_address.toUpperCase(),
        );
        if (!findTokenData) {
          this.setKeyValuePair(
            `${config.TOKEN_TYPE_ETH}`,
            token.token_address == null ? 'NativeCoin'.toUpperCase() : token.token_address.toUpperCase(),
            JSON.stringify(token)
          );
        }
      }

    } catch (err: any) {
      console.error('fetchERC20Tokens >>>>>', err);
      await global_helper.save_error_logs('ETH_fetchERC20Tokens', err.message)
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
      let token_data: any = await this.getKeyValuePair(config.TOKEN_TYPE_ETH, address);
      return JSON.parse(token_data);
    } catch (err: any) {
      console.error('check_our_wallet_address >>>>>', err);
      return null;
    }
  };


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
      let wallet_data: any = await this.getKeyValuePair(config.ETH_WALLET_ADDRESS, address);
      if (wallet_data) {
        console.log("wallet_Data>>>>>>>>", wallet_data)
      }
      return JSON.parse(wallet_data);
    } catch (error: any) {
      console.error('check_our_wallet_address >>>>>', error);
      return null;
    }
  };


  public behindBlocks = async () => {
    try {
      let behindBlocks: any = await EthOldBLockModel.findOne({
        attributes: ['id', 'block_number', 'start_block', 'end_block'],
        where: { status: 0 },
        raw: true,
        order: [['id', 'ASC']]
      })
      if (behindBlocks) {
        let add_data = {
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
}
const ethProcessHelper = new EthProcessHelper();
export default ethProcessHelper;