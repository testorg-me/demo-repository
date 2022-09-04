// module.exports.MONGO_CONNECTION_STRING = `mongodb://${mongoUser}:${mongoPassword}@ninjadev-db.slicepay.in:27017/${mongoDbName}`;
module.exports.MONGO_CONNECTION_STRING =
    "mongodb://fialseaospsvsc-dev:jRdXfkTaedtPdasemdS5mzdda4@sp-dddedwxv-sssfchasrsd-00-00-sydtfop.mongoddb.net:27017,sp-dev-shard-00-01-sytop.mongodb.net:27017,sp-dev-shard-00-02-sytop.mongodb.net:27017/buddydb?ssl=true&replicaSet=sp-dev-shard-0&authSource=admin&retryWrites=true&w=majority";

module.exports.HTTP_PORT = process.env.PORT || 8010;

