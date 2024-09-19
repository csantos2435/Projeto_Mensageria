const mongoose = require('mongoose')

const{
    MONGODB_USER,
    MONGODB_PASS,
    MONGODB_SERVER,
    MONGODB_DATABASE
} = process.env

module.exports = function() {
    // Conecta ao banco de dados
    mongoose.connect(`mongodb+srv://${MONGODB_USER}:${MONGODB_PASS}@${MONGODB_SERVER}/${MONGODB_DATABASE}?retryWrites=true&w=majority`, {
        useNewUrlParser: true,
        useUnifiedTopology: true
    })

    mongoose.connection.on('connected', () =>
        console.log('=> MONGOSE! CONECTADO COM SUCESSO AO SERVIDOR')
    )

    mongoose.connection.on('disconnected', () =>
        console.log('=> MONGOSE! DESCONECTADO DO SERVIDOR')
    )

    mongoose.connection.on('ERROR', error =>
        console.error('*** MONGOSE! ERRO AO SE CONECTAR AO SERVIDOR: ' + error)
    )

    // Quando for determinar o comando de interrupção Ctrl+C 
    process.on('SIGINT', () => {
        mongoose.connection.close(() => {
            console.log('=> MONGOSE! desconectando...')
            // Encerrar a aplicação sem erros
            process.exit(0)
        })
    })
}


//      Para conectar ao Banco MongoDB seguir as seguintes etapas:
//      -> Crie um arquivo chamado ".env" na pasta "BackEnd"
//      -> Adicione o seguinte código no arquivo criado, para configuração da conexão ao banco:
//              MONGODB_USER = Projeto_Mensageria
//              MONGODB_PASS = projetomensageria
//              MONGODB_SERVER = cluster0.dc2ih.mongodb.net
//              MONGODB_DATABASE = Projeto_Mensageria
//      -> Salve o arquivo, e execute o comando "npm start" no terminal
