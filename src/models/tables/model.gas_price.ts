import { DataTypes, Model, Optional } from "sequelize";

import { GasPriceModel } from "../interfaces/interface.gasprice";
import db from "../../config/db";


interface GasPriceCreationModel extends Optional<GasPriceModel, "id"> { }
interface WalletInstance extends Model<GasPriceModel, GasPriceCreationModel>, GasPriceModel { }

let dataObj = {
    id: {
        type: DataTypes.INTEGER,
        allowNull: false,
        primaryKey: true,
        autoIncrement: true
    },
    coin_family: {
        type: DataTypes.INTEGER,
        allowNull: false
    },
    safe_gas_price: {
        type: DataTypes.INTEGER,
        allowNull: false
    },
    propose_gas_price: {
        type: DataTypes.INTEGER,
        allowNull: false
    },
    fast_gas_price: {
        type: DataTypes.INTEGER,
        allowNull: false
    }
};

const GasPriceWrite = db.db_write.define<WalletInstance>('gas_prices', dataObj);
const GasPriceRead = db.db_read.define<WalletInstance>('gas_prices', dataObj);


export const GasPrice = {
    GasPriceWrite: GasPriceWrite,
    GasPriceRead: GasPriceRead,
};