const amazonProducts = require("./data/amazon-products.json");
const neo4j = require("neo4j-driver").v1;
const driver = neo4j.driver(
  process.env.NEO4J_URI,
  neo4j.auth.basic(process.env.NEO4J_USER, process.env.NEO4J_PASSWORD)
);
exports.driver = driver;

exports.quit = cb => {
  driver.close();
  console.log("Bye bye 👋");
  return;
};

exports.salute = cb => {
  console.log("Whats up? 🤘 🤘 🤘");
  return cb();
};

exports.insert_all = cb => {
  const session = driver.session();
  const start = process.hrtime();
  const queries = amazonProducts.map(product => {
    return session.run("CREATE (p:Product {id: $id}) RETURN p", {
      id: product.id
    });
  });
  Promise.all(queries)
    .then(result => {
      session.close();
      const end = process.hrtime(start);
      console.log(`Inserted ${queries.length} nodes 📦`);
      console.log(`⏰  %ds %dms`, end[0], end[1] / 1000000);
      return cb();
    })
    .catch(console.error);
};

exports.delete_all = cb => {
  const session = driver.session();
  session
    .run("MATCH(p:Product) DELETE p")
    .then(result => {
      session.close();
      console.log(` ❌ Deleted all product nodes`);
      return cb();
    })
    .catch(console.error);
};
