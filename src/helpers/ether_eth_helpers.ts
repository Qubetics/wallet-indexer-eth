import { ethers } from "ethers";
import { AbiItem, TokenData } from "../interfaces/ether.interface";

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


  public async tokenDetails(tokenAddress: string) {
    try {
      const contract = new ethers.Contract(tokenAddress, config.CONTRACT_ABI, this.provider);
      const [decimals, name, symbol] = await Promise.all([
        contract.decimals(),
        contract.name(),
        contract.symbol()
      ]);

      const data: TokenData = {
        decimals: Number(decimals),
        name,
        symbol
      };

      return data;
    } catch (err: any) {
      console.error("tokenDetails", err)
      return null;
    }
  }


}

export const ethEther = new EthEther();