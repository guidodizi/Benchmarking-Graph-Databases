const amazonProducts = require("../data/amazon-products.json");

exports.miw = (driver, handleError, cb) => {
  const session = driver.session();
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
  const start = process.hrtime();
  transaction
    .then(result => {
      session.close();
      const end = process.hrtime(start);
      console.log(`Inserted ${result.nodes} nodes  📦`);
      console.log(`Created ${result.relations} relations  🤝`);
      console.log(`⏰ Massive Insertion Workload: %ds %dms`, end[0], end[1] / 1000000);
      return cb();
    })
    .catch(handleError);
};

exports.siw = (driver, handleError, cb) => {
  const session = driver.session();
  let count = 0;
  let relations = 0;
  const nodesPromise = new Promise((resolve, reject) => {
    amazonProducts.map((product, i, arr) => {
      session
        .run("CREATE (p:Product {id: $id}) RETURN p", {
          id: product.id
        })
        .then(result => {
          count++;

          // register time
          if (count % 1000 === 0 && count > 0) {
            const end = process.hrtime(start);
            start = process.hrtime();
            console.log(`Inserted ${count} nodes  📦`);
            console.log(`⏰ Single Insertion Workload: %ds %dms`, end[0], end[1] / 1000000);
          }
          if (i === arr.length - 1) resolve(count);
        })
        .catch(reject);
    });
  });
  const relationsPromise = new Promise((resolve, reject) => {
    //create all relations
    amazonProducts
      .filter(product => product.related && product.related.length)
      .map(product => {
        return product.related.map(rel => ({ id: product.id, related: rel }));
      })
      .reduce((acc, curr) => acc.concat(curr), [])
      .map(({ id, related }, i, arr) => {
        session
          .run(
            `
            MATCH (p:Product {id: $id})
            MATCH (q:Product {id: $related})
            MERGE (p)-[:RELATED]->(q);
          `,
            { id, related }
          )
          .then(result => {
            count++;
            relations++;

            // register time
            if (count % 1000 === 0 && relations > 0) {
              const end = process.hrtime(start);
              start = process.hrtime();
              console.log(`Created ${relations} relations  🤝`);
              console.log(`⏰ Single Insertion Workload: %ds %dms`, end[0], end[1] / 1000000);
            }
            if (i === arr.length - 1) resolve();
          })
          .catch(reject);
      });
  });
  let start = process.hrtime();
  return nodesPromise
    .then(c => {
      console.log(`Inserted ${c} nodes`);
      return relationsPromise;
    })
    .then(() => console.log(`Inserted ${relations} nodes`))
    .then(() => {
      session.close();
      return cb();
    })
    .catch(handleError);
};

exports.queries = (driver, handleError, cb) => {
  const session = driver.session();
  const transaction = session.writeTransaction(tx => {
    // find neighbours (FN)
    tx.run(`
      MATCH(p:Product)-->(q)
      RETURN q
    `);

    // find adjacent nodes (FA)
    tx.run(`
      MATCH(p:Product)-[:RELATED]-(q)
      RETURN q
    `);

    // find shortest path (FS)
    const data = [...amazonProducts];
    const head = data.shift();
    const tail = data.sort(() => Math.random() - 0.5).slice(0, 100);
    tail.map(product => {
      tx.run(
        `
          MATCH (p:Product {id: $id}), (q:Product {id: $tail}),
            path = shortestpath((p)-[:RELATED*]-(q))
          RETURN path
        `,
        { id: head.id, tail: product.id }
      );
    });
  });
  let start = process.hrtime();
  transaction
    .then(() => {
      session.close();
      const end = process.hrtime(start);
      console.log(`⏰ Query Workload: %ds %dms`, end[0], end[1] / 1000000);
      return cb();
    })
    .catch(handleError);
};

exports.delete = (driver, handleError, cb) => {
  const session = driver.session();
  session
    .run("MATCH(p:Product) DETACH DELETE p")
    .then(result => {
      session.close();
      console.log(` ❌  Deleted all amazon graph`);
      return cb();
    })
    .catch(handleError);
};
