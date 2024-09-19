const mongoose = require('mongoose')

const schema = mongoose.Schema({
  uuid: {
    type: String,
    required: true  
  },
  created_at: {
    type: Date,
    required: true
  },
  type: {
    type: String,
    required: true
  },
  customer:{
    id:{
      type: Number,
      required: true 
    } ,
    name:{
      type: String,
      required: true
    }
  },
  items:[
    {
      id:{
        type: Number,
        required: true  
      },
      sku:{
          id:{
            type: String,
            required: true 
          },
          value:{
            type: Number,
            min: 0.01,
            require: true
          } 
      },
      quantity:{
          type: Number,
          require: true
      },
      category:{
          id:{
              type: String,
              required: true 
          },
          sub_category:{
              id:{
                  type: String,
                  required: true 
              },
          }
      }
    }
  ],
  totalvalue:{
    type: Number,
    required: true
  }
})

module.exports = mongoose.model('Order', schema, 'order')