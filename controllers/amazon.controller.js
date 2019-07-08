const amazonProducts = require("../data/amazon-products.json");

exports.miw_neo = (driver, handleError, cb) => {
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
      console.log(`⏰ Massive Insertion Workload: %ds %dms`, end[0], end[1] / 1000000);
      return cb();
    })
    .catch(handleError);
};

exports.siw_neo = (driver, handleError, cb) => {
  const session = driver.session();
  let start = process.hrtime();
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

exports.delete_neo = (driver, handleError, cb) => {
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

exports.miw_orient = (server, handleError, cb) => {
  const db = server.use(process.env.ORIENT_DB)
  const start = process.hrtime();
  var nodes = 0;
  var relations = 0;
  const first = amazonProducts.splice(0, 1)[0]
  var nodesCreated = {}
  var tx = db.let('node' + first.id, n => {
    // create all products
    n.create('vertex', 'V')
      .set({
        id: first.id,
      })
    nodes++;
  })
  nodesCreated['node' + first.id] = true
  amazonProducts.map(product => {
    tx = tx.let('node' + product.id, n => {
      // create all products
      n.create('vertex', 'V')
        .set({
          id: product.id,
        })
      nodes++;
    })
    nodesCreated['node' + product.id] = true
  });
  amazonProducts.splice(0, 0, first)
  console.log(nodesCreated)
  amazonProducts.map(product => {
    if (product.related) {
      product.related.map(rel => {
        // console.log('node' + rel)
        if (nodesCreated['node' + rel]) {
          tx = tx.let('ed', e => {
            e.create('EDGE', 'E')
              .from('$node' + product.id)
              .to('$node' + rel)
          })
          relations++;
        }
      })
    }
  })

  tx.commit().all()
    .then(_ => {
      db.close();
      const end = process.hrtime(start);
      console.log(`Inserted ${nodes} nodes  📦`);
      console.log(`Created ${relations} relations  🤝`);
      console.log(`⏰ Massive Insertion Workload: %ds %dms`, end[0], end[1] / 1000000);
      return cb();
    })
    .catch(handleError);
};

exports.siw_orient = (server, handleError, cb) => {
  const db = server.use(process.env.ORIENT_DB)
  let start = process.hrtime();
  let count = 0;
  let relations = 0;
  amazonProducts.map((product, i, arr) => {
    db
      .query(`CREATE VERTEX V SET id = ${product.id}`)
      .then(result => {
        count++;

        // register time
        if (count % 1000 === 0 && count > 0) {
          const end = process.hrtime(start);
          start = process.hrtime();
          console.log(`Inserted ${count} nodes  📦`);
          console.log(`⏰ Single Insertion Workload: %ds %dms`, end[0], end[1] / 1000000);
        }
        if (i === arr.length - 1) {
          console.log(`Inserted ${count} nodes`);
          amazonProducts
            .filter(product => product.related && product.related.length)
            .map(product => {
              return product.related.map(rel => ({ id: product.id, related: rel }));
            })
            .reduce((acc, curr) => acc.concat(curr), [])
            .map(({ id, related }, i, arr) => {
              db
                .query(`CREATE EDGE E FROM (SELECT FROM V WHERE id = ${id}) TO 
                (SELECT FROM V WHERE id = ${related})`)
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
                  if (i === arr.length - 1) {
                    console.log(`Inserted ${relations} nodes`)
                    db.close()
                    return cb()
                  }
                })
                .catch(err => {
                  if (err.message != 'No edge has been created because no target vertices\r\n\tDB name="BDNR"') {
                    console.log(err)
                  }
                });
            });
        }
      })
      .catch(err => {
        console.log(err)
      });
  });
}

exports.delete_orient = (server, handleError, cb) => {
  const db = server.use(process.env.ORIENT_DB)
  db.delete('VERTEX', 'V')
    .one()
    .then(
      function (del) {
        db.close()
        console.log(` ❌  Deleted all amazon graph`);
        return cb();
      }
    )
    .catch(handleError);
};