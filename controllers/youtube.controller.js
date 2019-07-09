const youtubeUsers = require("../data/youtube-users.json");
const youtubeGroups = require("../data/youtube-groups.json");

exports.miw = (driver, handleError, cb) => {
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

exports.siw = (driver, handleError, cb) => {
  const session = driver.session();
  let start = process.hrtime();
  let count = 0;
  let relations = 0;
  const usersPromise = new Promise((resolve, reject) => {
    youtubeUsers.map((user, i, arr) => {
      session
        .run("CREATE (p:User {id: $id}) RETURN p", {
          id: user.id
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
  const groupsPromise = new Promise((resolve, reject) => {
    youtubeGroups.map((group, i, arr) => {
      session
        .run("CREATE (p:Group {id: $id}) RETURN p", {
          id: group.id
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
    youtubeGroups
      .filter(group => group.members && group.members.length)
      .map(group => {
        return group.members.map(mem => ({ id: group.id, member: mem }));
      })
      .reduce((acc, curr) => acc.concat(curr), [])
      .map(({ id, member }, i, arr) => {
        session
          .run(
            `
            MATCH (u:User {id: $id})
            MATCH (g:Group {id: $member})
            MERGE (u)-[:MEMBER]->(g);
          `,
            { id, member }
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
  return usersPromise
    .then(c => {
      console.log(`Inserted ${c} nodes`);
      return groupsPromise;
    })
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

exports.delete = (driver, handleError, cb) => {
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
