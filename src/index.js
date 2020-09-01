#!/usr/bin/env node

const commander = require('commander')
const inquirer = require('inquirer')
const chalk = require('chalk')
const DynamoDB = require('aws-sdk/clients/dynamodb')

const cli = commander
  .name('dynamodb-query')
  .description('cli to query DynamoDB tables')
  .option('--region <region>')
  .option('--endpoint <endpoint>')
  .option('--page-size <pageSize>', 'page size', 25)
  .option('--table-name <tableName>')
  .option('--convert-empty-values')

cli.parse(process.argv)

const { region, endpoint, pageSize, convertEmptyValues, tableName } = cli.opts()

const dynamodbConfig = { region, endpoint, convertEmptyValues }

const dynamodb = {
  doc: new DynamoDB.DocumentClient(dynamodbConfig),
  db: new DynamoDB(dynamodbConfig)
}

async function run () {
  const { TableNames: tables } = await dynamodb.db.listTables().promise()

  const { table } = tableName
    ? { table: tableName }
    : await inquirer.prompt({
        type: 'list',
        name: 'table',
        message: 'Pick a table',
        choices: tables
      })

  const { type } = await inquirer.prompt({
    type: 'list',
    name: 'type',
    message: 'Type of operation',
    choices: ['Query', 'Scan']
  })

  const {
    Table: {
      AttributeDefinitions: attributeDefinitions,
      KeySchema: keySchema,
      GlobalSecondaryIndexes: globalSecondaryIndexes = []
    }
  } = await dynamodb.db
    .describeTable({
      TableName: table
    })
    .promise()

  const indexName = index => {
    const type = index.IndexName ? 'Index' : 'Table'
    const name = type === 'Table' ? table : index.IndexName
    const [pk, sk] = type === 'Table' ? index : index.KeySchema
    const names = [pk, sk].filter(Boolean).map(x => x.AttributeName)
    return `[${type}] ${name}: ${names.join(', ')}`
  }

  const indexes = [keySchema, ...globalSecondaryIndexes]

  const { index } = await inquirer.prompt({
    type: 'list',
    name: 'index',
    message: 'Pick an index',
    choices: indexes.map(indexName)
  })

  const indexOrTable = indexes.find(x => indexName(x) === index)

  const read = async args => {
    let exclusiveStartKey
    while (true) {
      const {
        Items: items,
        LastEvaluatedKey: lastEvaluatedKey
      } = await dynamodb.doc[type.toLowerCase()]({
        TableName: table,
        ...(indexOrTable.IndexName && { IndexName: indexOrTable.IndexName }),
        ...(exclusiveStartKey && { ExclusiveStartKey: exclusiveStartKey }),
        Limit: pageSize,
        ...args
      }).promise()

      for (const item of items) {
        console.table(chalk.cyan(JSON.stringify(item, null, 2)))
      }

      console.log(chalk.cyan(`${items.length} item(s) found`))
      if (!lastEvaluatedKey) break

      exclusiveStartKey = lastEvaluatedKey

      const { more = 'Y' } = await inquirer.prompt({
        name: 'more',
        message: 'There are more items available, load more [Y] ?',
        type: 'confirm'
      })
      if (more) {
        exclusiveStartKey = lastEvaluatedKey
      } else {
        break
      }
    }
  }

  if (type === 'Scan') {
    await read()
  } else {
    const [pk, sk] = indexOrTable.KeySchema
      ? indexOrTable.KeySchema
      : indexOrTable
    const pkType = attributeDefinitions.find(
      x => x.AttributeName === pk.AttributeName
    ).AttributeType
    const { value } = await inquirer.prompt({
      type: 'input',
      name: 'value',
      message: `Partition key (${pk.AttributeName})`
    })

    const pkValue = pkType === 'S' ? value : Number(value)

    const skType = sk
      ? attributeDefinitions.find(x => x.AttributeName === sk.AttributeName)
          .AttributeType
      : null

    const { skComparision = 'none' } = sk
      ? await inquirer.prompt({
          type: 'list',
          name: 'skComparision',
          message: `Sort key (${sk.AttributeName}) comparision`,
          choices: [
            'none',
            '=',
            '<',
            '<=',
            '>',
            '>=',
            'between',
            ...(skType === 'S' ? ['begins_with'] : [])
          ]
        })
      : {}

    const getValue = async message =>
      inquirer.prompt({ type: 'input', name: 'value', message })

    const skValues = (skComparision === 'none'
      ? []
      : skComparision === 'between'
      ? [
          await getValue(`${sk.AttributeName} from value`),
          await getValue(`${sk.AttributeName} to value`)
        ]
      : [await getValue(`${sk.AttributeName} value`)]
    ).map(x => (skType === 'S' ? x.value : Number(x.value)))

    const skKeyExpression = ['begins_with', 'between'].includes(skComparision)
      ? skValues.length === 1
        ? `${skComparision}(#sk, :sk)`
        : skValues.length === 2
        ? `#sk ${skComparision} :sk1 and :sk2`
        : ''
      : skValues.length === 1
      ? `#sk ${skComparision} :sk`
      : ''

    const keyConditionExpression = `#pk = :pk${
      skKeyExpression ? ` and ${skKeyExpression}` : ''
    }`

    const expressionAttributeNames = {
      '#pk': pk.AttributeName,
      ...(skValues.length && { '#sk': sk.AttributeName })
    }
    const expressionAttributeValues = {
      ':pk': pkValue,
      ...(skValues.length === 1 && { ':sk': skValues[0] }),
      ...(skValues.length === 2 && { ':sk1': skValues[0], ':sk2': skValues[1] })
    }

    await read({
      KeyConditionExpression: keyConditionExpression,
      ExpressionAttributeNames: expressionAttributeNames,
      ExpressionAttributeValues: expressionAttributeValues
    })
  }
}

run().catch(console.error)
