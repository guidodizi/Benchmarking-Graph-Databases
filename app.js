require("dotenv").config();
const inquirer = require("inquirer");
const controller = require("./controllers/controller");

const choices = [
  { name: "Say Hi", value: "salute", short: "salute" },
  new inquirer.Separator(),
  // Amazon
  {
    name: "🐳 (MIW) Create amazon graph",
    value: "miw_amazon",
    short: "massive insertion workload amazon"
  },
  {
    name: "🐁 (SIW) Create amazon graph",
    value: "siw_amazon",
    short: "single insertion workload amazon"
  },
  { name: "Delete amazon graph", value: "delete_youtube", short: "delete amazon" },
  new inquirer.Separator(),
  // Youtube
  {
    name: "🐳 (MIW) Create youtube graph",
    value: "miw_youtube",
    short: "massive insertion workload youtube"
  },
  {
    name: "🐁 (SIW) Create youtube graph",
    value: "siw_youtube",
    short: "single insertion workload youtube"
  },
  { name: "Delete youtube graph", value: "delete_youtube", short: "delete youtube" },
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
