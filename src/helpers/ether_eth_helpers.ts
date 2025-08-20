import { globalHelper } from "./global.helper"
import { ethers } from "ethers";
import { AbiItem } from "../interfaces/ether.interface";

import { config } from "../config/config";

class EthEther {
  public provider: ethers.JsonRpcProvider;
  public abi: AbiItem[] | string;

  constructor(abi: AbiItem[] | string = "") {
    this.provider = new ethers.JsonRpcProvider(config.NODE.ETH_RPC_URL || "");
    this.abi = abi;
    this.initialize();
  }

  public initialize() { }

  

  public async getERC20TokenBalance(
    token_address: string | null | undefined,
    tokenAbi: AbiItem[],
    wallet_address: string
  ) {
    try {
      if (!token_address) return 0;

      const contract = new ethers.Contract(token_address, tokenAbi, this.provider);
      const [decimals, balanceRaw] = await Promise.all([
        contract.decimals(),
        contract.balanceOf(wallet_address)
      ]);

      let tokenBalance: string;
      if (Number(decimals) === 18) {
        tokenBalance = ethers.formatUnits(balanceRaw, 18);
      } else {
        console.log("balanceRaw", balanceRaw);
        let tokenb = await globalHelper.bigNumberSafeMath(balanceRaw.toString(), "/", Math.pow(10, Number(decimals)));
        console.log("tokenb", tokenb);
        tokenBalance = tokenb?.toString() || "0";
      }
      return tokenBalance;
    } catch (err: any) {
      console.error("Error in get_erc20_token_balance_eth of eth.", err);
      throw err;
    }
  }

 
}

export const ethEther = new EthEther();