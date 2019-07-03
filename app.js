require("dotenv").config();
const inquirer = require("inquirer");
const controller = require("./controller");

const ask = question =>
  inquirer
    .prompt([
      {
        type: "list",
        name: "task",
        message: question,
        choices: [
          { name: "Say Hi", value: "salute", short: "salute" },
          { name: "Insert all amazon products", value: "insert_all", short: "insert all" },
          { name: "Delete all amazon products", value: "delete_all", short: "delete all" },
          { name: "Quit", value: "quit", short: "quit" }
        ],
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
