require("dotenv").config();
const inquirer = require("inquirer");
const controller = require("./controllers/controller");

const choices = [
  { name: "Say Hi", value: "salute", short: "salute" },
  new inquirer.Separator(),
  // Amazon operations
  {
    name: "🐳 (MIW) Create amazon graph",
    value: "miw_amazon_neo",
    short: "massive insertion workload amazon"
  },
  {
    name: "🐁 (SIW) Create amazon graph",
    value: "siw_amazon_neo",
    short: "single insertion workload amazon"
  },
  {
    name: "🔍 (QW) Queries on amazon graph",
    value: "queries_amazon_neo",
    short: "queries workload amazon"
  },
  { name: "Delete amazon graph", value: "delete_amazon_neo", short: "delete amazon" },
  new inquirer.Separator(),
  // Youtube operation
  {
    name: "🐳 (MIW) Create youtube graph",
    value: "miw_youtube_neo",
    short: "massive insertion workload youtube"
  },
  {
    name: "🐁 (SIW) Create youtube graph",
    value: "siw_youtube_neo",
    short: "single insertion workload youtube"
  },
  {
    name: "🔍 (QW) Queries on youtube graph",
    value: "queries_youtube_neo",
    short: "queries workload youtube"
  },
  { name: "Delete youtube graph", value: "delete_youtube_neo", short: "delete youtube" },
  new inquirer.Separator(),
  // Amazon Orient
  {
    name: "🐳 (MIW) Create amazon graph for OrientDB",
    value: "miw_amazon_orient",
    short: "massive insertion workload amazon OrientDB"
  },
  {
    name: "🐁 (SIW) Create amazon graph for OrientDB",
    value: "siw_amazon_orient",
    short: "single insertion workload amazon OrientDB"
  },
  {
    name: "Delete amazon graph for OrientDB",
    value: "delete_amazon_orient",
    short: "delete amazon OrientDB"
  },
  new inquirer.Separator(),
  // Youtube Orient
  {
    name: "🐳 (MIW) Create youtube graph for OrientDB",
    value: "miw_youtube_orient",
    short: "massive insertion workload youtube OrientDB"
  },
  {
    name: "🐁 (SIW) Create youtube graph for OrientDB",
    value: "siw_youtube_orient",
    short: "single insertion workload youtube OrientDB"
  },
  {
    name: "Delete youtube graph for OrientDB",
    value: "delete_youtube_orient",
    short: "delete youtube OrientDB"
  },
  new inquirer.Separator(),
  { name: "Quit", value: "quit", short: "quit" }
];

const ask = question =>
  inquirer
    .prompt([
      {
        type: "list",
        name: "task",
        message: question,
        pageSize: choices.length,
        choices,
        filter: function(val) {
          return val.toLowerCase();
        }
      }
    ])
    .then(answers => {
      return controller[answers.task](cb);
    });

const cb = () => {
  console.log("\n");
  return ask("What else can we do for you?");
};

ask("What would you like to do?");
