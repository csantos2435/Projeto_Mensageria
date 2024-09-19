const express = require('express')
const router = express.Router()
const controller = require('../controllers/order')

router.post('/', controller.create)
router.get('/', controller.retrieveAll)
router.get('/:id', controller.retrieveOne)
router.get('/:id', controller.retrieveOneIdCustomer)
router.get('/:id', controller.retrieveOneIdProduct)
router.delete('/:id', controller.delete)

module.exports = router
