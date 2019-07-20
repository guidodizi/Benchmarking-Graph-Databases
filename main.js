require("dotenv").config();
const controller = require("./controllers/controller");

try{
  console.log('\ndelete_amazon_neo')
  controller['delete_amazon_neo'](function() {
    console.log('\nmiw_amazon_neo')
    controller['miw_amazon_neo'](function() {
      console.log('\ndelete_amazon_neo')
      controller['delete_amazon_neo'](function() {
        console.log('\nsiw_amazon_neo')
        controller['siw_amazon_neo'](function() {
          console.log('\nqueries_amazon_neo')
          controller['queries_amazon_neo'](function() {
            console.log('\ndelete_amazon_orient')
            controller['delete_amazon_orient'](function() {
              console.log('\nmiw_amazon_orient')
              controller['miw_amazon_orient'](function() {
                console.log('\ndelete_amazon_orient')
                controller['delete_amazon_orient'](function() {
                  console.log('\nsiw_amazon_orient')
                  controller['siw_amazon_orient'](function() {
                    console.log('\nqueries_amazon_orient')
                    controller['queries_amazon_orient'](function() {
                      process.exit()
                    })
                  })
                })
              })
            })
          })
        })
      })
    })
  })
} catch (err) {
  console.log(err)
}