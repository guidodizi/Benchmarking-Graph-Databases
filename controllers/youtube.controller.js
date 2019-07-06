const youtubeUsers = require("../data/youtube-users.json");
const youtubeGroups = require("../data/youtube-groups.json");

exports.miw_neo = (driver, handleError, cb) => {
  const session = driver.session();
  const start = process.hrtime();
  const transaction = session.writeTransaction(tx => {
    let nodes = 0;
    let relations = 0;
    
    // create all users
    youtubeUsers.map(user => {
      tx.run("CREATE (u:User {id: $id}) RETURN u", {
        id: user.id
      });
      nodes++;
    });
    // create all groups
    youtubeGroups.map(group => {
      tx.run("CREATE (g:Group {id: $id}) RETURN g", {
        id: group.id
      });
      nodes++;
    });

    //create all relations
    youtubeUsers.map(user => {
      if (user.belongs) {
        user.belongs.map(gr => {
          tx.run(
            `
              MATCH (u:User {id: $id})
              MATCH (g:Group {id: $gr})
              MERGE (u)-[:BELONGS]->(g);
            `,
            {
              id: user.id,
              gr
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
  // const session = driver.session();
  // let start = process.hrtime();
  // let count = 0;
  // let relations = 0;
  // const nodesPromise = new Promise((resolve, reject) => {
  //   amazonProducts.map((product, i, arr) => {
  //     session
  //       .run("CREATE (p:Product {id: $id}) RETURN p", {
  //         id: product.id
  //       })
  //       .then(result => {
  //         count++;

  //         // register time
  //         if (count % 1000 === 0 && count > 0) {
  //           const end = process.hrtime(start);
  //           start = process.hrtime();
  //           console.log(`Inserted ${count} nodes  📦`);
  //           console.log(`⏰ Single Insertion Workload: %ds %dms`, end[0], end[1] / 1000000);
  //         }
  //         if (i === arr.length - 1) resolve(count);
  //       })
  //       .catch(reject);
  //   });
  // });
  // const relationsPromise = new Promise((resolve, reject) => {
  //   //create all relations
  //   amazonProducts
  //     .filter(product => product.related && product.related.length)
  //     .map(product => {
  //       return product.related.map(rel => ({ id: product.id, related: rel }));
  //     })
  //     .reduce((acc, curr) => acc.concat(curr), [])
  //     .map(({ id, related }, i, arr) => {
  //       session
  //         .run(
  //           `
  //           MATCH (p:Product {id: $id})
  //           MATCH (q:Product {id: $related})
  //           MERGE (p)-[:RELATED]->(q);
  //         `,
  //           { id, related }
  //         )
  //         .then(result => {
  //           count++;
  //           relations++;

  //           // register time
  //           if (count % 1000 === 0 && relations > 0) {
  //             const end = process.hrtime(start);
  //             start = process.hrtime();
  //             console.log(`Created ${relations} relations  🤝`);
  //             console.log(`⏰ Single Insertion Workload: %ds %dms`, end[0], end[1] / 1000000);
  //           }
  //           if (i === arr.length - 1) resolve();
  //         })
  //         .catch(reject);
  //     });
  // });

  // return nodesPromise
  //   .then(c => {
  //     console.log(`Inserted ${c} nodes`);
  //     return relationsPromise;
  //   })
  //   .then(() => console.log(`Inserted ${relations} nodes`))
  //   .then(() => {
  //     session.close();
  //     return cb();
  //   })
  //   .catch(handleError);
};

exports.delete_neo = (driver, handleError, cb) => {
  const session = driver.session();
  session
    .run("MATCH(u:User) MATCH(g:Group) DETACH DELETE u, g")
    .then(result => {
      session.close();
      console.log(` ❌  Deleted all product nodes`);
      return cb();
    })
    .catch(handleError);
};
