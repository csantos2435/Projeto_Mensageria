const { PubSub } = require('@google-cloud/pubsub');
const fetch = require('node-fetch');

// Cria um cliente do Pub/Sub
const pubSubClient = new PubSub();

/**
 * Insere os dados no MongoDB através da API de inserção.
 * @param {Object} data - Os dados a serem inseridos.
 */
async function insertData(data) {
  const response = await fetch('http://localhost:3000/orders', {
    method: 'POST',
    body: JSON.stringify(data),
    headers: {
      'Content-Type': 'application/json',
    },
  });

  if (!response.ok) {
    console.error(`Erro ao inserir dados: ${response.statusText}`);
  } else {
    console.log('Dados inseridos com sucesso!');
  }
}

/**
 * Escuta as mensagens do Pub/Sub.
 * @param {string} subscriptionNameOrId - Nome ou ID da assinatura.
 * @param {number} timeout - Tempo limite em segundos.
 */
function listenForMessages(subscriptionNameOrId, timeout) {
  const subscription = pubSubClient.subscription(subscriptionNameOrId);
  let messageCount = 0;

  const messageHandler = async (message) => {
    console.log(`Received message ${message.id}:`);
    const data = JSON.parse(message.data.toString()); // Parseia os dados recebidos
    console.log(`\tData:`, data);
    
    // Insere os dados no MongoDB
    await insertData(data);
    
    messageCount += 1;
    message.ack(); // Confirma a recepção da mensagem
  };

  subscription.on('message', messageHandler);

  // Remove o listener após o timeout
  setTimeout(() => {
    subscription.removeListener('message', messageHandler);
    console.log(`${messageCount} message(s) received.`);
  }, timeout * 1000);
}

// Defina sua assinatura e timeout aqui
const subscriptionNameOrId = '1aceb58abd458a559dfb00e229467638db6c3eee'; 
const timeout = 60; // Tempo limite em segundos

listenForMessages(subscriptionNameOrId, timeout);
