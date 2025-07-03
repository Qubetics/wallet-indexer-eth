import { BooleanEnum, TokenStandard } from "../enum";
import { utilityHelper } from "./utility.helper";
import { config } from '../config';
import { ethers } from 'ethers';

class BlockChainHelper {
    public provider: ethers.JsonRpcProvider;

    constructor() {
        this.provider = new ethers.JsonRpcProvider(config.config.NODE.ETH_RPC_URL || "");
        this.initialize();
    }

    public initialize() { }

    convertWeiToEth = async (value: number | string) => {
        try {
            const valueAsString = value.toString();
            return ethers.formatEther(valueAsString);;
        } catch (error) {
            console.error(`ConvertWeiToEth error >>`, error);
            return 0;
        }
    }

    fetchBalance = async (
        address: string,
        coin: any
    ) => {
        try {
            let balance: number = 0;
            if (coin.is_token == BooleanEnum.true) {
                switch (coin.token_type) {
                    case TokenStandard.ERC20:
                        balance = await this.erc20TokenFetchBalance(
                            address, //// wallet_address
                            coin.token_address ? coin.token_address : null/// contract_address
                        )
                        break;
                    case TokenStandard.ERC721:
                        balance = await this.erc721TokenFetchBalance(
                            address, //// wallet_address
                            coin.token_address ? coin.token_address : null /// contract_address
                        )
                        break;
                    default:
                        balance = 0
                        break;
                }
            } else {
                balance = await this.coinFetchBalance(address);
            }
            return balance
        } catch (error: any) {
            console.error(`Eth_Helper BSC_Fetch_Balance error >>`, error);
            return null;
        }
    }

    // public ERC20_Token_Fetch_Balance = async (
    public erc20TokenFetchBalance = async (
        wallet_address: string,
        contract_address: string | null
    ) => {
        try {
            if (contract_address) {
                const contract = new ethers.Contract(
                    contract_address,
                    config.config.CONTRACT_ABI,
                    this.provider
                );
                const decimals = Number(await contract.decimals())
                const balance = await contract.balanceOf(wallet_address);
                const tokenBalance = await utilityHelper.bigNumberSafeMath(
                    balance.toString(),
                    "/",
                    Math.pow(10, decimals)
                );
                const tokBalance: string = await utilityHelper.exponentialToDecimal(tokenBalance)
                return Number(tokBalance);
            }
            return 0;
        } catch (error) {
            console.error(`Eth_Helper erc20TokenFetchBalance error >>`, error);

            throw error;
        }
    }

    // private ERC721_Token_Fetch_Balance = async (
    private erc721TokenFetchBalance = async (
        wallet_address: string,
        contract_address: string | null
    ) => {
        try {
            if (contract_address) {
                const contract = new ethers.Contract(
                    contract_address,
                    config.config.ERC721ABI,
                    this.provider
                );
                const balance = await contract.balanceOf(wallet_address);
                return Number(balance.toString());
            }
            return 0;
        } catch (error) {
            console.error(`Eth_Helper erc721TokenFetchBalance error >>`, error);
            throw error;
        }
    }

    // public Coin_Fetch_Balance = async (
    public coinFetchBalance = async (
        address: string
    ) => {
        try {
            const balance = await this.provider.getBalance(address);
            const tokenBalance = await utilityHelper.bigNumberSafeMath(
                balance.toString(),
                "/",
                Math.pow(10, 18)
            );
            const coinBalance: string = await utilityHelper.exponentialToDecimal(tokenBalance)
            return Number(coinBalance || 0);

        } catch (error: any) {
            console.error(`Eth_Helper coinFetchBalance error >>`, error);
            throw error;
        }
    }



}
export let blockChainHelper = new BlockChainHelper();