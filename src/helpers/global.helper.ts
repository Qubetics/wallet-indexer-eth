import axios from "axios";
import { config } from "../config/config";
import { catchErrorMsgsModel, CoinInterface } from "../models";
import { walletModel, customTokennModel } from "../models/tables/index"
import ethProcessHelper from "../processes/process.helper";
import { coinsModel } from "../models/tables";
import redisClient from "../config/redis";
import { Op } from "sequelize";
import { ethEther } from "./ether_eth_helpers";
import { AbiItem } from "../interfaces/ether.interface";
import kafkaHelper from "../config/kafka.helper";



class GlobalHelper {
    //public async check_coin_in_wallet(wallet_address: string, coin: CoinInterface, balance: any) {
    public async checkCoinInWallet(wallet_address: string, coin: CoinInterface, balance: any): Promise<boolean> {
        try {
            // Input validation
            if (!wallet_address) {
                console.error("wallet_address is required");
                return false;
            }

            if (!coin || !coin.coin_id) {
                console.error("coin with coin_id is required");
                return false;
            }

            // Check if wallet exists
            let wallet_data: any = await walletModel.findOne({
                attributes: ["user_id"],
                where: {
                    wallet_address: wallet_address,
                    coin_id: coin.coin_id,
                },
                raw: true
            });

            if (!wallet_data) {
                // Get wallet data from Redis
                wallet_data = await ethProcessHelper.check_our_wallet_address(wallet_address);

                if (!wallet_data || !wallet_data.user_id) {
                    console.error(`No wallet data found for address: ${wallet_address}`);
                    return false;
                }

                // Create new wallet record
                const data_obj: any = {
                    user_id: wallet_data.user_id,
                    wallet_name: wallet_data.wallet_name,
                    wallet_address: wallet_address,
                    coin_id: coin.coin_id,
                    coin_family: coin.coin_family,
                    balance: balance !== null ? balance : 0,
                    // default_wallet: 1,
                    status: 1
                };

                await walletModel.create(data_obj);
            }

            // Handle custom token creation if needed
            if (coin.added_by === 'user') {
                await this.handleCustomTokenCreation(wallet_data.user_id, coin.coin_id);
            }

            return true;
        } catch (err: any) {
            console.error("Error in checkCoinInWallet:", err);
            return false;
        }
    }

    private async handleCustomTokenCreation(user_id: number, coin_id: number): Promise<void> {
        try {
            // Check if custom token already exists
            const user_exist: any = await customTokennModel.findOne({
                attributes: ["id"],
                where: { user_id, coin_id },
                raw: true
            });

            if (!user_exist) {
                // Create custom token record
                const currentUTCDate: string = new Date()
                    .toISOString()
                    .replace(/T/, " ")
                    .replace(/\..+/, "");

                await customTokennModel.create({
                    coin_id: coin_id,
                    user_id: user_id,
                    created_at: currentUTCDate,
                    updated_at: currentUTCDate
                });
            }
        } catch (err: any) {
            console.error("Error in handleCustomTokenCreation:", err);
        }
    }
    public async sendMail(email: string, text: string, subject: string) {
        try {
            const MAILGUN_API_KEY: string = `${config.MAILGUN.MAILGUN_API_KEY}`;
            const MAILGUN_DOMAIN: string = `${config.MAILGUN.MAILGUN_DOMAIN}`;
            const MAILGUN_BASE_URL: string = `${config.MAILGUN.MAILGUN_BASE_URL}${MAILGUN_DOMAIN}`;
            const response: any = await axios({
                method: 'post',
                url: `${MAILGUN_BASE_URL}/messages`,
                auth: {
                    username: 'api',
                    password: MAILGUN_API_KEY
                },
                params: {
                    from: config.MAILGUN.FROM_GMAIL,
                    to: email,
                    subject: subject,
                    html: text
                }
            });
        } catch (err: any) {
            console.error("sendMail", err)
            await this.saveErrorLogs("MAILGUN_ERROR", err.message)
        }
    }
    // public async save_error_logs(fn_name: string, error_message: any) {
    public async saveErrorLogs(fn_name: string, error_message: any) {
        try {
            let value_exist: any = await ethProcessHelper.getKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.ETH_ERROR_LOGS)
            if (value_exist) {
                if (Number(value_exist) == 0) {
                    await catchErrorMsgsModel.create({
                        fx_name: fn_name,
                        error_msg: error_message || {}
                    })
                    await ethProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.ETH_ERROR_LOGS, '1')
                } else {
                    console.log("value other than 0 exist")
                }
            } else {
                await ethProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.ETH_ERROR_LOGS, '0')
                console.log("value does not exist")
            }
        } catch (err: any) {
            console.error("Error in saveErrorLogs", err)
        }
    }
    


    //TODO : Token  image  from  cmc  by  symbol  logic  added


    public async bigNumberSafeMath (
        c: string,
        operation: string,
        d: string | number,
        precision?: number
      ) {
        BigNumber.config({ DECIMAL_PLACES: 18 });
        var a = new BigNumber(c);
        var b = new BigNumber(typeof d === 'number' ? d.toString() : d);
        var rtn;
        // Figure out which operation to perform.
        switch (operation.toLowerCase()) {
          case '-':
            rtn = a.minus(b);
            break;
          case '+':
            rtn = a.plus(b);
            break;
          case '*':
          case 'x':
            rtn = a.multipliedBy(b);
            break;
          case '÷':
          case '/':
            rtn = a.dividedBy(b);
            break;
          default:
            break;
        }
        return rtn;
      };


    public async cmcApi(params: any) {
        try {
            console.log("Entered into cmcApi params params", params)
            let url: string = '';
            let header: any = {};
            let result: any;
          
                    console.log("In use CoinZone")
                    url = `${config.COIN_ZONE.URL}${params}`;
                    header = { "api_key": `${config.COIN_ZONE.KEY}` }
                    result = await axios({
                        method: "get",
                        url: url,
                        headers: header,
                    });
                
            return result;
        } catch (err: any) {
            console.error("Error in cmcApi cmcApi cmcApi >>>", err);
            throw err;
        }
    }

    public async UpdateCmcImgUrl(image_url: string) {
        try {
            console.log('image_url >>>', image_url);
            if (image_url?.includes("img/coins/16x16/")) {
                image_url = image_url.replace('16x16', '200x200');
            } else if (image_url?.includes("img/coins/32x32/")) {
                image_url = image_url.replace('32x32', '200x200');
            } else if (image_url?.includes("img/coins/64x64/")) {
                image_url = image_url.replace('64x64', '200x200');
            } else if (image_url?.includes("img/coins/128x128/")) {
                image_url = image_url.replace('128x128', '200x200');
            }
            return image_url;
        } catch (err: any) {
            console.error(`UpdateCmcImgUrl error>>> `, err)
            return null;
        }
    }

    public STATIC_COIN_FAMILY_SYMBOL = {
        1: "BNB",
        2: "ETH",
        3: "BTC",
        4: "POL",
        5: "SOL",
        6: "TRX",
        7: "TICS"
}


public STATIC_COIN_FAMILY_NAME = {
    1: ["BNB"],
    2: ["Ethereum"],
    3: ["BTC"],
    4: ["POL (prev. MATIC)", "Polygon", "MATIC"], 
    5: ["Solana"],
    6: ["TRON", "Tron"],
    7: ["TICS"]
  }

//Get  token  image  from  cmc  by  symbol 
public async getTokenImageFromCmcBySymbol(data: any) {
    try {
        const tokenAddress = data.tokenAddress;
        const coinFamily = data.coinFamily as keyof typeof this.STATIC_COIN_FAMILY_SYMBOL;
        const  PlatformName = this.STATIC_COIN_FAMILY_NAME[coinFamily];

        if(PlatformName == null){
            return { image: null, id: null };
        }

        let params: any = `info?symbol=${data.symbol}`;

        let result: any = await this.cmcApi(params);
        let id: any = null;
        let imageUrl: any = null;
        let name: any = null;
        let symbol: any = null;

        
        if (result.data && result.data.data) {
            let image: any;
            const symbolKey = Object.keys(result.data.data)[0];
            
            if (symbolKey) {
                // Get all tokens with this symbol
                const tokens = result.data.data[symbolKey];
                
                // Find the token that matches both symbol and token address
                for (const token of tokens) {

                    if (token.contract_address) {
                        // Check each contract address entry
                        for (const contract of token.contract_address) {

                            if (contract?.platform && 
                                contract?.platform?.coin && 
                                PlatformName.includes(contract?.platform?.coin?.name) && 
                                contract?.contract_address?.toLowerCase() == tokenAddress.toLowerCase()) {
                                id = token.id;
                                image = token.logo;
                                name = token.name;
                                symbol = token.symbol;
                                break;
                            }
                        }
                        if (id) break; // Break outer loop if match found
                    }
                }
                
                if (image) {
                    imageUrl = await this.UpdateCmcImgUrl(image);
                }
            }
        }
       
        return { image: imageUrl, id: id, name: name, symbol: symbol };
    } catch (err: any) {
        console.error("Error in getTokenImageFromCmcBySymbol", err);
        return null;
    }
}


public async getTradingSource(symbol: string): Promise<any> {
    console.log('symbol symbol', symbol);
    if (!symbol) return { status: false, graphSource: null, graphPair: '' }; // Exit if no symbol provided

    const exchanges = [
        { name: 'BINANCE', url: (symbol: string) => `https://api.binance.com/api/v3/exchangeInfo?symbol=${symbol}USDT` },
        { name: 'OKX', url: (symbol: string) => `https://www.okx.com/api/v5/market/ticker?instId=${symbol}-USDT` },
        { name: 'BYBIT', url: (symbol: string) => `https://api.bybit.com/v5/market/instruments-info?category=spot&symbol=${symbol}USDT` },
        { name: 'BITGET', url: (symbol: string) => `https://api.bitget.com/api/v2/spot/public/symbols?symbol=${symbol}USDT` },
        { name: 'BITMART', url: (symbol: string) => `https://api-cloud.bitmart.com/spot/quotation/v3/ticker?symbol=${symbol}_USDT` },
        { name: 'COINBASE', url: (symbol: string) => `https://api.coinbase.com/api/v3/brokerage/market/products/?product_ids=${symbol}-USDT` },
        { name: 'MEXC', url: (symbol: string) => `https://api.mexc.com/api/v3/exchangeInfo?symbol=${symbol}USDT` },
        { name: 'GATEIO', url: (symbol: string) => `https://api.gateio.ws/api/v4/spot/currency_pairs/${symbol}_USDT` }
    ];
    const usdtExchanges = [
        { name: 'BINANCE', url: (symbol: string) => `https://api.binance.com/api/v3/exchangeInfo?symbol=${symbol}USD` },
        { name: 'OKX', url: (symbol: string) => `https://www.okx.com/api/v5/market/ticker?instId=${symbol}-USD` },
        { name: 'BYBIT', url: (symbol: string) => `https://api.bybit.com/v5/market/instruments-info?category=spot&symbol=${symbol}USD` },
        { name: 'BITGET', url: (symbol: string) => `https://api.bitget.com/api/v2/spot/public/symbols?symbol=${symbol}USD` },
        { name: 'BITMART', url: (symbol: string) => `https://api-cloud.bitmart.com/spot/quotation/v3/ticker?symbol=${symbol}_USD` },
        { name: 'COINBASE', url: (symbol: string) => `https://api.coinbase.com/api/v3/brokerage/market/products/?product_ids=${symbol}-USD` },
        { name: 'MEXC', url: (symbol: string) => `https://api.mexc.com/api/v3/exchangeInfo?symbol=${symbol}USD` },
        { name: 'GATEIO', url: (symbol: string) => `https://api.gateio.ws/api/v4/spot/currency_pairs/${symbol}_USD` }

    ];

    const symbolUpper = symbol.toUpperCase();
    const graphPair = symbolUpper == 'USDT' ? `${symbolUpper}USD` : `${symbolUpper}USDT`;

    // Helper function to find the exchange source
    const getExchangeSource = async (symbol: string) => {
        const exchangesSource = symbol?.toUpperCase() == 'USDT' ? usdtExchanges : exchanges;
        for (const exchange of exchangesSource) {
            try {
                const response = await axios.get(exchange.url(symbol));

                // Simplified exchange response validation
                if (isValidExchangeResponse(exchange, response)) {
                    return exchange.name;
                }
            } catch (error: any) {
                console.error(`Error fetching from ${exchange.name}:`, error.message);
            }
        }
        return null; // No exchange source found
    };

    // Function to validate the response based on exchange name
    const isValidExchangeResponse = (exchange: { name: string }, response: any) => {
        switch (exchange.name) {
            case 'BINANCE':
                return response.data?.code === undefined;
            case 'OKX':
                return response.data?.code === "0";
            case 'BYBIT':
                return response.data?.result?.list?.length > 0;
            case 'BITGET':
                return response.data.code === '00000' && response.data.data?.length > 0;
            case 'BITMART':
                return response?.data?.code === 1000 && response?.data?.data !== undefined;
            case 'COINBASE':
                return response?.data?.products?.length > 0;
            case 'MEXC':
                return response?.data?.symbols?.length > 0;
            case 'GATEIO':
                return response?.data?.label === undefined;
            default:
                return false;
        }
    };

    const graphSource = await getExchangeSource(symbolUpper);
    console.log('graphSource symbol', graphSource);
    return {
        status: graphSource !== null,
        graphSource: graphSource,
        graphPair: graphPair
    };
}

public async coinCreate(obj: any) {
    try {
        let data: any = await coinsModel.create(obj);
        return data;
    } catch (err: any) {
        console.error("Error in coin_create>>", err);

        const uniqueConstraint =
            err.name === 'SequelizeUniqueConstraintError' &&
            err.errors?.some(
                (e: any) => e.path === 'unique_coinfamily_tokenaddress' || e.message?.includes('unique_coinfamily_tokenaddress')
            );

        if (uniqueConstraint) {
            const tokenAddress = obj.token_address;
            const coinFamily = obj.coin_family;

            try {
                const existing = await coinsModel.findOne({
                    where: {
                        token_address: tokenAddress,
                        coin_family: coinFamily,
                    },
                    raw: true
                });
                console.log("🚀 ~ CoinQueries ~ coin_create ~ existing:", existing)

                if (existing) {
                    return existing;
                }
            } catch (findErr) {
                console.error("Error finding existing coin>>", findErr);
                await this.saveErrorLogs("coin_create_find_existing", (findErr as any).message);
            }
        }

        await this.saveErrorLogs("coin_create", err.message);
        return null;
    }
}

public async setHashTable(key: string, field: any, value: any, expiryInSeconds?: number) {
    try {
      await redisClient.HSET(key, field.toUpperCase(), value, (err: any, reply: any) => {
        if (err) {
          console.error('redis set string data error...', err)
        }
      });
      if (expiryInSeconds) {
        await redisClient.EXPIRE(key, expiryInSeconds);
      }
    } catch (err: any) {
      console.error('💥 ~ ~ set_hash_table error');
      throw err;
    }
  }

public async updateTokenInRedisHash(coin_data: any) {
    try {
          await this.setHashTable(
            config.TOKENLIST.ETH.ERC20,
            coin_data.token_address,
            JSON.stringify(coin_data)
          );

      }catch (err: any) {
      console.error("Error in update_token_in_redis_hash.", err);
      await this.saveErrorLogs(
        "update_token_in_redis_hash",
        err.message
      );
      return null;
    }

  }

  public async customTokenCreate(obj: any) {
    try {
        let data: any = await customTokennModel.create(obj)
        return data;
    } catch (err: any) {
        console.error("Error in custom_token_create>>", err)
        await this.saveErrorLogs("custom_token_create", err.message);
        return null;
    }
}

public async walletFindOneWithOrder(attr: any, where_clause: any, order: any) {
    try {
        let data: any = await walletModel.findOne({
            attributes: attr,
            where: where_clause,
            order: order,
            raw: true
        })
        return data;
    } catch (err: any) {
        console.error("Error in wallet_find_one_with_order>>", err)
        await this.saveErrorLogs("wallet_find_one_with_order", err.message);
        return null;
    }
}

public async exponentialToDecimal(exponential: number | string) {
    let decimal: string = exponential.toString().toLowerCase();
    if (decimal.includes('e+')) {
      const exponentialSplitted: any = decimal.split('e+');
      let postfix: any = '';
      for (
        let i: number = 0;
        i <
        +exponentialSplitted[1] -
        (exponentialSplitted[0].includes('.')
          ? exponentialSplitted[0].split('.')[1].length
          : 0);
        i++
      ) {
        postfix += '0';
      }
      const addCommas: any = (text: string) => {
        let j: number = 3;
        let textLength: number = text.length;
        while (j < textLength) {
          text = `${text.slice(0, textLength - j)}${text.slice(
            textLength - j,
            textLength
          )}`;
          textLength++;
          j += 3 + 1;
        }
        return text;
      };
      decimal = addCommas(exponentialSplitted[0].replace('.', '') + postfix);
    }
    if (decimal.toLowerCase().includes('e-')) {
      const exponentialSplitted: any = decimal.split('e-');
      let prefix: any = '0.';
      for (let i = 0; i < +exponentialSplitted[1] - 1; i++) {
        prefix += '0';
      }
      decimal = prefix + exponentialSplitted[0].replace('.', '');
    }
    return decimal;
  };

public async getNewTokenBalance(coinFamily: number, walletAddress: string, tokenAddress: string) {
    try {
        let tokenBalance: any;
        let userBalance: string = '';

                tokenBalance = (await ethEther.getERC20TokenBalance(tokenAddress, config.CONTRACT_ABI as AbiItem[], walletAddress))?.toString();
                userBalance = await this.exponentialToDecimal(Number(tokenBalance))
             
        return { status: true, balance: userBalance };
    }
    catch (err: any) {
        console.error("Error in get_new_token_balance.", err)
        await this.saveErrorLogs("get_new_token_balance", err.message);
        return { status: false, balance: '0' };
    }
}

public async createWalletForCoin(
    walletAddress: string,
    coinId: number,
    walletName: string,
    userId: number,
    coinFamily: number,
    lang: string,
    currentUTCDate: any,
    tokenAddress: string,
    isToken: number,
    tokenType: string
  ) {
    try {
      let message: any;
      let wallet_data: any = await this.walletFindOneWithOrder(
        ["wallet_id", "status"],
        { wallet_address: { [Op.like]: walletAddress }, coin_id: coinId },
        [["wallet_id", "ASC"]]
      );
      if (!wallet_data) {
        let tokenBalData: any = await this.getNewTokenBalance(
          coinFamily,
          walletAddress,
          tokenAddress
        );

        const saveWalletData = {
          wallet_address: walletAddress,
          wallet_name: walletName,
          coin_id: coinId,
          user_id: userId,
          balance: tokenBalData.status ? tokenBalData.balance : "0",
          default_wallet: 1,
          coin_family: coinFamily,
          status: 1,
          created_at: currentUTCDate,
          updated_at: currentUTCDate,
        };

        
        await globalHelper.walletCreate(saveWalletData);
        message = "TOKEN_ADDED";
        if (tokenBalData.status == false) {
          // add to queue
          await kafkaHelper.produceMessage(config.BACKEND_WALLET_ADDRESSES, {
            coin_data: {
              coin_id: coinId,
              coin_family: coinFamily,
              is_token: isToken,
              token_address: tokenAddress,
              token_type: tokenType,
            },
            wallet_address: walletAddress,
            queue_count: 0,
          });
        }
      } else {
        if (wallet_data.status == 1) {
          message = "TOKEN_EXIST";
        } else {
          await this.walletUpdate(
            { status: 1 },
            { wallet_address: walletAddress, coin_id: coinId }
          );
          message = "TOKEN_ADDED";
        }
      }
      return message;
    } catch (err: any) {
      console.error("error at create_wallet_for_coin================>", err);
      await this.saveErrorLogs("create_wallet_for_coin", err.message);
      return null;
    }
  }

  public async walletCreate(obj: any) {
    try {
        let data: any = await walletModel.create(obj)
        return data;
    } catch (err: any) {
        console.error("Error in wallet_create>>", err)
        await this.saveErrorLogs("wallet_create", err.message);
        return null;
    }
}

public async walletUpdate(set: any, where_clause: any) {
    try {
        let data: any = await walletModel.update(set, { where: where_clause })
        return data;
    } catch (err: any) {
        console.error("Error in wallet_update>>", err)
        await this.saveErrorLogs("wallet_update", err.message);
        return null;
    }
}
    

}
export let globalHelper = new GlobalHelper();
