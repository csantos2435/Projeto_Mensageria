const Order = require('../models/Order')

const controller = {}   

controller.create = async (req, res) => {
  try {
    await Order.create(req.body)

    res.status(201).end()
  }
  catch(error) {
    console.error(error)
    res.status(500).send(error)
  }
}

controller.retrieveAll = async (req, res) => {
  try {
    const result = await Order.find()
    res.send(result)
  }
  catch(error) {
    console.error(error)
    res.status(500).send(error)
  }
}

//Filtro por UUID do Pedido
controller.retrieveOne = async(req, res) => {
  try {
    const result = await Order.findById(req.params.uuid)

    if(result) {
      res.send(result)
    }
    else {
      res.status(404).end()
    }
  }
  catch(error) {
    console.error(error)
    res.status(500).send(error)
  }
}

//Filtro por ID do Cliente
controller.retrieveOneIdCustomer = async(req, res) => {
  const idParametro = req.params.customer.id;

  try {
    const result = await Order.findOne({id: idParametro}).exec();
    
    if(result) {
      res.send(result)
    }
    else {
      res.status(404).end()
    }
  }
  catch(error) {
    console.error(error)
    res.status(500).send(error)
  }
}

//Filtro por ID do Produto
controller.retrieveOneIdProduct = async(req, res) => {
  const idParametro = req.params.items.id;

  try {
    const result = await Order.findOne({id: idParametro}).exec();
    
    if(result) {
      res.send(result)
    }
    else {
      res.status(404).end()
    }
  }
  catch(error) {
    console.error(error)
    res.status(500).send(error)
  }
}

controller.delete = async (req, res) => {
  try {
    const result = await Order.findByIdAndDelete(req.params.id)
    if(result) {
      res.status(204).end()
    }
    else {
      res.status(404).end()
    }
  }
  catch(error) {
    console.error(error)
    res.status(500).send(error)
  }
}

module.exports = controller
