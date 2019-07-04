const amazonProducts = require("./data/amazon-products.json");
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

exports.create_amazon = cb => {
  const session = driver.session();
  const start = process.hrtime();
  const transaction = session.writeTransaction(tx => {
    let nodes = 0;
    let relations = 0;
    // create all products
    amazonProducts.map(product => {
      tx.run("CREATE (p:Product {id: $id}) RETURN p", {
        id: product.id
      });
      nodes++;
    });

    //create all relations
    amazonProducts.map(product => {
      if (product.related) {
        product.related.map(rel => {
          tx.run(
            `
            MATCH (p:Product {id: $id})
            MATCH (q:Product {id: $rel})
            MERGE (p)-[:RELATED]->(q);
          `,
            {
              id: product.id,
              rel
            }
          );
          relations++;
        });
      }
    });

    return { nodes, relations };
  });
  transaction
    .then(result => {
      session.close();
      const end = process.hrtime(start);
      console.log(`Inserted ${result.nodes} nodes  📦`);
      console.log(`Created ${result.relations} relations  🤝`);
      console.log(`⏰  %ds %dms`, end[0], end[1] / 1000000);
      return cb();
    })
    .catch(handleError);
};

exports.delete_amazon = cb => {
  const session = driver.session();
  session
    .run("MATCH(p:Product) DETACH DELETE p")
    .then(result => {
      session.close();
      console.log(` ❌  Deleted all product nodes`);
      return cb();
    })
    .catch(handleError);
};


exports.create_youtube = cb => {
  
}