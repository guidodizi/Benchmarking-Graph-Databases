const amazonController = require("./amazon.controller");
const youtubeController = require("./youtube.controller");

const neo4j = require("neo4j-driver").v1;
const driver = neo4j.driver(
  process.env.NEO4J_URI,
  neo4j.auth.basic(process.env.NEO4J_USER, process.env.NEO4J_PASSWORD),
  { maxTransactionRetryTime: 30000 }
);

const handleError = err => {
  console.error(err);
  return cb();
};

exports.quit = cb => {
  driver.close();
  console.log("Bye bye 👋");
  return;
};

exports.salute = cb => {
  console.log("Whats up? 🤘 🤘 🤘");
  return cb();
};

// Amazon
exports.miw_amazon = cb => amazonController.miw(driver, handleError, cb);

exports.siw_amazon = cb => amazonController.siw(driver, handleError, cb);

exports.delete_amazon = cb => amazonController.delete(driver, handleError, cb);

// Youtube
exports.miw_youtube = cb => youtubeController.miw(driver, handleError, cb);

exports.siw_youtube = cb => youtubeController.siw(driver, handleError, cb);

exports.delete_youtube = cb => youtubeController.delete(driver, handleError, cb);
