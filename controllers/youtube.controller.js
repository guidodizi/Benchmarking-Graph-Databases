const youtubeUsers = require("../data/youtube-users.json");
const youtubeGroups = require("../data/youtube-groups.json");
const { asyncForEach } = require("../utils/async");

const async = require("async");

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
              MERGE (g)-[:MEMBER]->(u);
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

exports.siw_neo = async (driver, handleError, cb) => {
  const session = driver.session();
  let count = 0;
  let relations = 0;
  let start = process.hrtime();

  const usersCount = await new Promise((resolve, reject) => {
    asyncForEach(youtubeUsers, async (user, i, arr) => {
      await session
        .run("CREATE (p:User {id: $id}) RETURN p", {
          id: user.id
        })
        .catch(reject);

      count++;
      // register time
      if (count % 1000 === 0 && count > 0) {
        const end = process.hrtime(start);
        start = process.hrtime();
        console.log(`Inserted ${count} nodes  📦`);
        console.log(`⏰ Single Insertion Workload: %ds %dms`, end[0], end[1] / 1000000);
      }
      if (i === arr.length - 1) resolve(count);
    });
  }).catch(handleError);
  console.log(`Inserted ${usersCount} nodes`);

  const groupsCount = await new Promise((resolve, reject) => {
    asyncForEach(youtubeGroups, async (group, i, arr) => {
      await session
        .run("CREATE (p:Group {id: $id}) RETURN p", {
          id: group.id
        })
        .catch(reject);

      count++;
      // register time
      if (count % 1000 === 0 && count > 0) {
        const end = process.hrtime(start);
        start = process.hrtime();
        console.log(`Inserted ${count} nodes  📦`);
        console.log(`⏰ Single Insertion Workload: %ds %dms`, end[0], end[1] / 1000000);
      }
      if (i === arr.length - 1) resolve(count);
    });
  }).catch(handleError);
  console.log(`Inserted ${groupsCount} nodes`);

  const relationsCount = await new Promise((resolve, reject) => {
    //create all relations
    const relationsList = youtubeGroups
      .filter(group => group.members && group.members.length)
      .map(group => {
        return group.members.map(mem => ({ id: group.id, member: mem }));
      })
      .reduce((acc, curr) => acc.concat(curr), []);
    asyncForEach(relationsList, async ({ id, member }, i, arr) => {
      await session
        .run(
          `
            MATCH (u:User {id: $id})
            MATCH (g:Group {id: $member})
            MERGE (u)-[:MEMBER]->(g);
          `,
          { id, member }
        )
        .catch(reject);

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
    });
  }).catch(handleError);
  console.log(`Inserted ${relationsCount} relations  🤝`);

  session.close();
  return cb();
};

exports.delete_neo = (driver, handleError, cb) => {
  const session = driver.session();
  session
    .run("MATCH(u:User) MATCH(g:Group) DETACH DELETE u, g")
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
  async.eachLimit(
    youtubeUsers,
    1000,
    function(user, callback) {
      tx = tx.let("user" + user.id, n => {
        n.create("vertex", "U").set({
          id: user.id
        });
        nodes++;
        callback();
      });
    },
    function(err) {
      youtubeUsers.splice(0, 0, first);

      // create all groups
      async.eachLimit(
        youtubeGroups,
        1000,
        function(group, callback) {
          tx = tx.let("group" + group.id, n => {
            n.create("vertex", "G").set({
              id: group.id
            });
            nodes++;
            callback();
          });
        },
        function(err) {
          //create all relations
          async.eachLimit(
            youtubeUsers,
            1000,
            function(user, callback) {
              if (user.belongs) {
                async.eachLimit(
                  user.belongs,
                  1000,
                  function(gr, callback2) {
                    tx = tx.let("ed", e => {
                      e.create("EDGE", "E")
                        .from("$user" + user.id)
                        .to("$group" + gr);
                    });
                    relations++;
                    callback2();
                  },
                  function(err) {
                    callback();
                  }
                );
              }
            },
            function(err) {
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
            }
          );
        }
      );
    }
  );
};

exports.siw_orient = async (server, handleError, cb) => {
  const db = server.use(process.env.ORIENT_DB);
  let start = process.hrtime();
  let count = 0;
  var users = {};
  var groups = {};

  await Promise.all([db.query("CREATE CLASS U EXTENDS V"), db.query("CREATE CLASS G EXTENDS V")]);

  // create all users
  youtubeUsers.map((user, i, arr) => {
    console.log("user", user.id);
    db.query(`CREATE VERTEX U SET id = ${user.id}`)
      .then(u => {
        users[user.id] = u;
        count++;

        // register time
        if (count % 1000 === 0 && count > 0) {
          const end = process.hrtime(start);
          start = process.hrtime();
          console.log(`Inserted ${count} users  📦`);
          console.log(`⏰ Single Insertion Workload: %ds %dms`, end[0], end[1] / 1000000);
        }
        if (i === arr.length - 1) {
          console.log(`Inserted ${count} users`);

          count = 0;
          youtubeGroups.map((group, i, arr) => {
            db.query(`CREATE VERTEX G SET id = ${group.id}`)
              .then(g => {
                groups[group.id] = g;
                count++;

                // register time
                if (count % 1000 === 0 && count > 0) {
                  const end = process.hrtime(start);
                  start = process.hrtime();
                  console.log(`Inserted ${count} nodes  📦`);
                  console.log(`⏰ Single Insertion Workload: %ds %dms`, end[0], end[1] / 1000000);
                }
                if (i === arr.length - 1) {
                  console.log(`Inserted ${count} groups`);

                  count = 0;
                  //create all relations
                  youtubeUsers.map((user, i, arr) => {
                    if (user.belongs) {
                      user.belongs.map(gr => {
                        db.create("EDGE", "E")
                          .from(users[user.id])
                          .to(groups[gr])
                          .then(_ => {
                            count++;
                            if (count % 1000 === 0 && count > 0) {
                              const end = process.hrtime(start);
                              start = process.hrtime();
                              console.log(`Created ${count} relations  🤝`);
                              console.log(
                                `⏰ Single Insertion Workload: %ds %dms`,
                                end[0],
                                end[1] / 1000000
                              );
                            }
                          })
                          .catch(handleError);
                      });
                    }
                    if (i === arr.length - 1) {
                      console.log(`Inserted ${count} relations`);
                      db.close();
                      return cb();
                    }
                  });
                }
              })
              .catch(handleError);
          });
        }
      })
      .catch(handleError);
  });
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
