const amazonController = require("./amazon.controller");
const youtubeController = require("./youtube.controller");

const OrientDB = require("orientjs");
const neo4j = require("neo4j-driver").v1;
const driver = neo4j.driver(
  process.env.NEO4J_URI,
  neo4j.auth.basic(process.env.NEO4J_USER, process.env.NEO4J_PASSWORD),
  { maxTransactionRetryTime: 30000 }
);
const server = OrientDB({
  host: process.env.ORIENT_URI,
  port: process.env.ORIENT_PORT,
  username: process.env.ORIENT_USER,
  password: process.env.ORIENT_PASSWORD
});

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
exports.miw_amazon_neo = cb => amazonController.miw_neo(driver, handleError, cb);

exports.siw_amazon_neo = cb => amazonController.siw_neo(driver, handleError, cb);

exports.queries_amazon_neo = cb => amazonController.queries_neo(driver, handleError, cb);

exports.delete_amazon_neo = cb => amazonController.delete_neo(driver, handleError, cb);

exports.miw_amazon_orient = cb => amazonController.miw_orient(server, handleError, cb);

exports.siw_amazon_orient = cb => amazonController.siw_orient(server, handleError, cb);

exports.queries_amazon_orient = cb => amazonController.queries_orient(server, handleError, cb);

exports.delete_amazon_orient = cb => amazonController.delete_orient(server, handleError, cb);

// Youtube
exports.miw_youtube_neo = cb => youtubeController.miw_neo(driver, handleError, cb);

exports.siw_youtube_neo = cb => youtubeController.siw_neo(driver, handleError, cb);

exports.delete_youtube_neo = cb => youtubeController.delete_neo(driver, handleError, cb);

exports.miw_youtube_orient = cb => youtubeController.miw_orient(server, handleError, cb);

exports.siw_youtube_orient = cb => youtubeController.siw_orient(server, handleError, cb);

exports.delete_youtube_orient = cb => youtubeController.delete_orient(server, handleError, cb);
