require("dotenv").config();
const controller = require("./controllers/controller");

try{
  console.log('AMAZON')
  console.log('\n delete all')
  controller['delete_amazon_neo'](function() {
    controller['delete_amazon_orient'](function() {
      controller['delete_youtube_neo'](function() {
        controller['delete_youtube_orient'](function() {
          console.log('\n miw_amazon_neo')
          controller['miw_amazon_neo'](function() {
            console.log('\n delete_amazon_neo')
            controller['delete_amazon_neo'](function() {
              console.log('\n siw_amazon_neo')
              controller['siw_amazon_neo'](function() {
                console.log('\n queries_amazon_neo')
                controller['queries_amazon_neo'](function() {
                  console.log('\n delete_amazon_orient')
                  controller['delete_amazon_orient'](function() {
                    console.log('\n miw_amazon_orient')
                    controller['miw_amazon_orient'](function() {
                      console.log('\n delete_amazon_orient')
                      controller['delete_amazon_orient'](function() {
                        console.log('\n siw_amazon_orient')
                        controller['siw_amazon_orient'](function() {
                          console.log('\n queries_amazon_orient')
                          controller['queries_amazon_orient'](function() {
                            console.log('\n delete_amazon_neo para empezar youtube')
                            controller['delete_amazon_neo'](function() {
                              console.log('\n delete_amazon_orient para empezar youtube')
                              controller['delete_amazon_orient'](function() {
                                console.log('\n\n\n YOUTUBE')
                                console.log('\n miw_youtube_neo')
                                controller['miw_youtube_neo'](function() {
                                  console.log('\n delete_youtube_neo')
                                  controller['delete_youtube_neo'](function() {
                                    console.log('\n siw_youtube_neo')
                                    controller['siw_youtube_neo'](function() {
                                      console.log('\n queries_youtube_neo')
                                      controller['queries_youtube_neo'](function() {
                                        console.log('\n miw_youtube_orient')
                                        controller['miw_youtube_orient'](function() {
                                          console.log('\n delete_youtube_orient')
                                          controller['delete_youtube_orient'](function() {
                                            console.log('\n siw_youtube_orient')
                                            controller['siw_youtube_orient'](function() {
                                              console.log('\n queries_youtube_orient')
                                              controller['queries_youtube_orient'](function() {
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
      })
    })
  })
} catch (err) {
  console.log(err)
}