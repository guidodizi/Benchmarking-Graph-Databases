const amazonProducts = require("../data/amazon-test.json");
const { asyncForEach } = require("../utils/async");
const async = require('async');

var data = [...amazonProducts];
var head = data.shift();
var tail = data.sort(() => Math.random() - 0.5).slice(0, 100);

exports.miw_neo = (driver, handleError, cb) => {
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
              MERGE (p)-[:SIMILAR]->(q);
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

exports.siw_neo = async (driver, handleError, cb) => {
  const session = driver.session();
  let count = 0;
  let relations = 0;
  let start = process.hrtime();
  let firststart = process.hrtime();

  async.eachSeries(amazonProducts, (product, callback) => {
    session
      .run("CREATE (p:Product {id: $id}) RETURN p", {
        id: product.id
      })
      .then(() => {
        count++;
        // register time
        if (count % 1000 === 0 && count > 0) {
          const end = process.hrtime(start);
          start = process.hrtime();
          console.log(`Inserted ${count} nodes  📦`);
          console.log(`⏰ Single Insertion Workload: %ds %dms`, end[0], end[1] / 1000000);
        }
        callback()
      })
      .catch(err => {
        callback(err)
      });
  }, function (err) {
    if (err) return cb(err)
    console.log(`Inserted ${count} nodes`);

    //create all relations
    const relationsList = amazonProducts
      .filter(product => product.related && product.related.length)
      .map(product => {
        return product.related.map(rel => ({ id: product.id, related: rel }));
      })
      .reduce((acc, curr) => acc.concat(curr), []);
    async.eachSeries(relationsList, ({ id, related }, callback2) => {
      session
        .run(
          `
            MATCH (p:Product {id: $id})
            MATCH (q:Product {id: $related})
            MERGE (p)-[:RELATED]->(q);
          `,
          { id, related }
        )
        .then(() => {
          count++;
          relations++;
          // register time
          if (count % 1000 === 0 && relations > 0) {
            const end = process.hrtime(start);
            start = process.hrtime();
            console.log(`Created ${relations} relations  🤝`);
            console.log(`⏰ Single Insertion Workload: %ds %dms`, end[0], end[1] / 1000000);
          }
          callback2()
        })
        .catch(err => {
          callback2(err)
        });
    }, function (err) {
      if (err) return cb(err)
      const end = process.hrtime(firststart);
      console.log(`Inserted ${relations} relations  🤝`);
      console.log(`⏰ Single Insertion Workload: %ds %dms`, end[0], end[1] / 1000000);
      session.close();
      return cb();
    })
  })
};

exports.queries_neo = (driver, handleError, cb) => {
  const session = driver.session();

  // find neighbours (FN)
  let start1 = process.hrtime();
  session
    .run(`
      MATCH(p:Product)-->(q)
      RETURN q
    `)
    .then(() => {
      const end1 = process.hrtime(start1);
      console.log(`⏰ FN: %ds %dms`, end1[0], end1[1] / 1000000);

      // find adjacent nodes (FA)
      let start2 = process.hrtime();
      session
        .run(`
        MATCH(p:Product)-[:RELATED]-(q)
        RETURN p, q
        `)
        .then(() => {
          const end2 = process.hrtime(start2);
          console.log(`⏰ FA: %ds %dms`, end2[0], end2[1] / 1000000);

          // find shortest path (FS)
          let start3 = process.hrtime();
          async.eachSeries(tail, function (product, callback) {
            session
              .run(`
                MATCH (p:Product {id: $id}), (q:Product {id: $tail}),
                  path = shortestpath((p)-[:RELATED*]-(q))
                RETURN path
                `,
                { id: head.id, tail: product.id })
              .then(_ => {
                callback()
              })
              .catch(err => {
                callback(err)
              })
          }, function (err) {
            if (err) return handleError
            const end3 = process.hrtime(start3);
            console.log(`⏰ FS: %ds %dms`, end3[0], end3[1] / 1000000);
            session.close();
            return cb();
          })
        })
        .catch(handleError)
    })
    .catch(handleError)
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
  const db = server.use(process.env.ORIENT_DB);
  const start = process.hrtime();
  var nodes = 0;
  var relations = 0;
  const first = amazonProducts.splice(0, 1)[0];
  var nodesCreated = {};
  var tx = db.let("node" + first.id, n => {
    // create all products
    n.create("vertex", "V").set({
      id: first.id
    });
    nodes++;
  });
  nodesCreated["node" + first.id] = true;
  amazonProducts.map(product => {
    tx = tx.let("node" + product.id, n => {
      // create all products
      n.create("vertex", "V").set({
        id: product.id
      });
      nodes++;
    });
    nodesCreated["node" + product.id] = true;
  });
  amazonProducts.splice(0, 0, first);
  amazonProducts.map(product => {
    if (product.related) {
      product.related.map(rel => {
        // console.log('node' + rel)
        if (nodesCreated["node" + rel]) {
          tx = tx.let("ed", e => {
            e.create("EDGE", "E")
              .from("SELECT FROM V WHERE id =" + product.id)
              .to("SELECT FROM V WHERE id =" + rel);
          });
          relations++;
        }
      });
    }
  });

  tx.commit()
    .all()
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

exports.siw_orient = async (server, handleError, cb) => {
  const db = server.use(process.env.ORIENT_DB);
  let start = process.hrtime();
  let firststart = process.hrtime();
  let count = 0;
  let relations = 0;

  async.eachSeries(amazonProducts, (product, callback) => {
    db.query(`CREATE VERTEX V SET id = ${product.id}`)
      .then(() => {
        count++;
        if (count % 1000 === 0 && count > 0) {
          const end = process.hrtime(start);
          start = process.hrtime();
          console.log(`Inserted ${count} nodes  📦`);
          console.log(`⏰ Single Insertion Workload: %ds %dms`, end[0], end[1] / 1000000);
        }
        callback()
      })
      .catch(err => {
        callback(err)
      });
  }, function (err) {
    if (err) return cb(err)
    console.log(`Inserted ${count} nodes`);

    const relationsList = amazonProducts
      .filter(product => product.related && product.related.length)
      .map(product => {
        return product.related.map(rel => ({ id: product.id, related: rel }));
      })
      .reduce((acc, curr) => acc.concat(curr), []);
    async.eachSeries(relationsList, ({ id, related }, callback2) => {
      db.query(`CREATE EDGE E FROM (SELECT FROM V WHERE id = ${id}) TO (SELECT FROM V WHERE id = ${related})`)
        .then(() => {
          count++;
          relations++;
          // register time
          if (count % 1000 === 0 && relations > 0) {
            const end = process.hrtime(start);
            start = process.hrtime();
            console.log(`Created ${relations} relations  🤝`);
            console.log(`⏰ Single Insertion Workload: %ds %dms`, end[0], end[1] / 1000000);
          }
          callback2()
        })
        .catch(err => {
          console.log(err)
          callback2(err)
        });
    }, function (err) {
      if (err) return cb(err)
      const end = process.hrtime(firststart);
      console.log(`Inserted ${relations} relations  🤝`);
      console.log(`⏰ Single Insertion Workload: %ds %dms`, end[0], end[1] / 1000000);
      db.close();
      return cb();
    })
  })
};

exports.queries_orient = (server, handleError, cb) => {
  const db = server.use(process.env.ORIENT_DB);

  // find neighbours (FN)
  let start1 = process.hrtime();
  db.query(`SELECT FROM V WHERE IN().size() > 0`)
  .then(() => {
    const end1 = process.hrtime(start1);
    console.log(`⏰ FN: %ds %dms`, end1[0], end1[1] / 1000000);

    // find adjacent nodes (FA)
    let start2 = process.hrtime();
    db.query(`SELECT FROM V WHERE IN().size() > 0 OR OUT().size() > 0`)
    .then(() => {
      const end2 = process.hrtime(start2);
      console.log(`⏰ FA: %ds %dms`, end2[0], end2[1] / 1000000);

      // find shortest path (FS)
      let start3 = process.hrtime();
      async.eachSeries(tail, function (product, callback) {
        db.query(`SELECT shortestPath((SELECT FROM V WHERE id = ${head.id}), 
          (SELECT FROM V WHERE id = ${product.id})) AS path
        `)
        .then(_ => {
          callback()
        })
        .catch(err => {
          callback(err)
        })
      }, function (err) {
        if (err) return handleError
        const end3 = process.hrtime(start3);
        console.log(`⏰ FS: %ds %dms`, end3[0], end3[1] / 1000000);
        db.close();
        return cb();
      })
    })
    .catch(handleError)
  })
  .catch(handleError)
};

exports.delete_orient = (server, handleError, cb) => {
  const db = server.use(process.env.ORIENT_DB);
  db.delete("EDGE", "E")
    .one()
    .then(function (del) {
      db.delete("VERTEX", "V")
        .one()
        .then(function (del) {
          db.close();
          console.log(` ❌  Deleted all amazon graph`);
          return cb();
        })
        .catch(handleError);
    })
    .catch(handleError);
};
