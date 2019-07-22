const youtubeUsers = require("../data/youtube-users.json");
const youtubeGroups = require("../data/youtube-groups-less.json");
const { asyncForEach } = require("../utils/async");

const async = require("async");
var head = [...youtubeUsers].shift();
var tail = [...youtubeGroups].sort(() => Math.random() - 0.5).slice(0, 100);

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
    youtubeGroups.map(group => {
      if (group.members) {
        group.members.map(u => {
          tx.run(
            `
              MATCH (g:Group {id: $id})
              MATCH (u:User {id: $u})
              MERGE (u)-[:MEMBER]->(g);
            `,
            {
              id: group.id,
              u
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
  let count = 0;
  let relations = 0;
  let start = process.hrtime();
  let firststart = process.hrtime();

  async.eachSeries(youtubeUsers, function(user, callback) {
    session
    .run("CREATE (u:User {id: $id}) RETURN u", {
      id: user.id
    })
    .then(() => {
      count++;
      // register time
      if (count % 1000 === 0 && count > 0) {
        const end = process.hrtime(start);
        start = process.hrtime();
        console.log(`Inserted ${count} user nodes  📦`);
        console.log(`⏰ Single Insertion Workload: %ds %dms`, end[0], end[1] / 1000000);
      }
      callback()
    })
    .catch(err => {
      callback(err)
    });
  }, function(err) {
    if (err) return cb(err)
    console.log(`Inserted ${count} user nodes`);
    count = 0

    async.eachSeries(youtubeGroups, function(group, callback2) {
      session
      .run("CREATE (g:Group {id: $id}) RETURN g", {
        id: group.id
      })
      .then(() => {
        count++;
        // register time
        if (count % 1000 === 0 && count > 0) {
          const end = process.hrtime(start);
          start = process.hrtime();
          console.log(`Inserted ${count} group nodes  📦`);
          console.log(`⏰ Single Insertion Workload: %ds %dms`, end[0], end[1] / 1000000);
        }
        callback2()
      })
      .catch(err => {
        callback2(err)
      });
    }, function(err) {
      if (err) return cb(err)

      console.log(`Inserted ${count} group nodes`);
      count = 0
      const relationsList = youtubeGroups
      .filter(group => group.members && group.members.length)
      .map(group => {
        return group.members.map(mem => ({ id: group.id, member: mem }));
      })
      .reduce((acc, curr) => acc.concat(curr), []);

      async.eachSeries(relationsList, function({ id, member }, callback3) {
        session
        .run(
          `
            MATCH (g:Group {id: $id})
            MATCH (u:User {id: $u})
            MERGE (u)-[:MEMBER]->(g);
          `,
          { id: id, u: member }
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
          callback3()
        })
        .catch(err => {
          callback3(err)
        });
      }, function(err) {
        if (err) return cb(err)
        const end = process.hrtime(firststart);
        console.log(`Inserted ${relations} relations  🤝`);
        console.log(`⏰ Single Insertion Workload: %ds %dms`, end[0], end[1] / 1000000);
        session.close();
        return cb();
      })
    })
  })
};

exports.queries_neo = (driver, handleError, cb) => {
  const session = driver.session();

  // find neighbours (FN)
  let start1 = process.hrtime();
  session
    .run(`
      MATCH(u:User)-->(g)
      RETURN g
    `)
    .then(() => {
      const end1 = process.hrtime(start1);
      console.log(`⏰ FN: %ds %dms`, end1[0], end1[1] / 1000000);

      // find adjacent nodes (FA)
      let start2 = process.hrtime();
      session
        .run(`
        MATCH(u:User)-[:MEMBER]-(g)
        RETURN u, g
        `)
        .then(() => {
          const end2 = process.hrtime(start2);
          console.log(`⏰ FA: %ds %dms`, end2[0], end2[1] / 1000000);

          // find shortest path (FS)
          let start3 = process.hrtime();
          async.eachSeries(tail, function (group, callback) {
            session
              .run(`
                MATCH (u:User {id: $id}), (g:Group {id: $tail}),
                  path = shortestpath((u)-[:MEMBER*]-(g))
                RETURN path
                `,
                { id: head.id, tail: group.id })
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
    .run("MATCH (n) DETACH DELETE n")
    .then(result => {
      session.close();
      console.log(` ❌  Deleted all youtube graph`);
      return cb();
    })
    .catch(handleError);
};

exports.miw_orient = async (server, handleError, cb) => {
  const db = server.use(process.env.ORIENT_DB);
  const start = process.hrtime();
  const first = youtubeUsers.splice(0, 1)[0];
  let nodes = 0;
  let relations = 0;
  // var nodesCreated = {}

  await Promise.all([db.query("CREATE CLASS U EXTENDS V"), db.query("CREATE CLASS G EXTENDS V")]);

  // create first user
  var tx = db.let("user" + first.id, n => {
    n.create("vertex", "U").set({
      id: first.id
    });
    nodes++;
  });
  // nodesCreated['user' + first.id] = true

  // create all users
  youtubeUsers.map(user => {
    tx = tx.let("user" + user.id, n => {
      n.create("vertex", "U").set({
        id: user.id
      });
      nodes++;
    });
  })
  youtubeUsers.splice(0, 0, first);
  
  // create all groups
  youtubeGroups.map(group => {
    tx = tx.let("group" + group.id, n => {
      n.create("vertex", "G").set({
        id: group.id
      });
      nodes++;
    });
  })

  //create all relations
  youtubeGroups.map(group => {
    if (group.members) {
      group.members.map(u => {
        tx = tx.let("ed", e => {
          e.create("EDGE", "E")
            .from("SELECT FROM U WHERE id = " + u)
            .to("SELECT FROM G WHERE id = " + group.id);
        });
        relations++;
      });
    }
  })

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

  await Promise.all([db.query("CREATE CLASS U EXTENDS V"), db.query("CREATE CLASS G EXTENDS V")]);

  // create all users
  async.eachSeries(youtubeUsers, function(user, callback) {
    db.query(`CREATE VERTEX U SET id = ${user.id}`)
    .then(u => {
      count++;

      // register time
      if (count % 1000 === 0 && count > 0) {
        const end = process.hrtime(start);
        start = process.hrtime();
        console.log(`Inserted ${count} user nodes 📦`);
        console.log(`⏰ Single Insertion Workload: %ds %dms`, end[0], end[1] / 1000000);
      }
      callback()
    })
    .catch(err => {
      callback(err)
    })
  }, function(err) {
    if (err) return cb(err)

    console.log(`Inserted ${count} user nodes`);
    count = 0

    async.eachSeries(youtubeGroups, function(group, callback2) {
      db.query(`CREATE VERTEX G SET id = ${group.id}`)
      .then(g => {
        count++;

        // register time
        if (count % 1000 === 0 && count > 0) {
          const end = process.hrtime(start);
          start = process.hrtime();
          console.log(`Inserted ${count} group nodes  📦`);
          console.log(`⏰ Single Insertion Workload: %ds %dms`, end[0], end[1] / 1000000);
        }
        callback2()
      })
      .catch(err => {
        console.log(err)
        callback2(err)
      })
    }, function(err) {
      if (err) return cb(err)

      console.log(`Inserted ${count} group nodes`);
      count = 0

      const relationsList = youtubeGroups
      .filter(group => group.members && group.members.length)
      .map(group => {
        return group.members.map(mem => ({ id: group.id, member: mem }));
      })
      .reduce((acc, curr) => acc.concat(curr), []);

      async.eachSeries(relationsList, function({ id, member }, callback3) {
        db.query(`CREATE EDGE E FROM (SELECT FROM U WHERE id = ${member}) 
          TO (SELECT FROM G WHERE id =  ${id})`)
        .then(_ => {
          count++;
          relations++;
          // register time
          if (count % 1000 === 0 && relations > 0) {
            const end = process.hrtime(start);
            start = process.hrtime();
            console.log(`Created ${relations} relations  🤝`);
            console.log(`⏰ Single Insertion Workload: %ds %dms`, end[0], end[1] / 1000000);
          }
          callback3()
        })
        .catch(err => {
          callback3(err)
        })
      }, function(err) {
        if (err) return cb(err)
        const end = process.hrtime(firststart);
        console.log(`Inserted ${relations} relations  🤝`);
        console.log(`⏰ Single Insertion Workload: %ds %dms`, end[0], end[1] / 1000000);
        db.close();
        return cb();
      })
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
      async.eachSeries(tail, function (group, callback) {
        db.query(`SELECT shortestPath((SELECT FROM U WHERE id = ${head.id}), 
          (SELECT FROM G WHERE id = ${group.id})) AS path
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

exports.delete_orient = async (server, handleError, cb) => {
  const db = server.use(process.env.ORIENT_DB);
  try {
    await db.delete("VERTEX", "G").one();
  } catch (e) {}
  try {
    await db.delete("VERTEX", "U").one();
  } catch (e) {}
  try {
    await db.query("DROP CLASS G");
  } catch (e) {}
  db.query("DROP CLASS U")
    .then(function(del) {
      db.close();
      console.log(` ❌  Deleted all youtube graph`);
      return cb();
    })
    .catch(e => {
      console.log(` ❌  Deleted all youtube graph`);
      return cb();
    });
};
