const { gql, ApolloServer } = require("apollo-server");
const { Neo4jGraphQL } = require("@neo4j/graphql");
const neo4j = require("neo4j-driver");
require("dotenv").config();

const typeDefs = gql`
  type Patient {
    USUBJID: String!
    AGE: Int
    BMI: Float
    SEX: String!
    ARM: String!
    Treatment: [Treatment!]! @relationship(type: "WAS_TREATED", direction: IN)
  }

  type Treatment {
    Name: String!
    Patient: [Treatment!]! @relationship(type: "WAS_TREATED", direction: OUT)
  }
`;

const driver = neo4j.driver(
  process.env.NEO4J_URI,
  neo4j.auth.basic(process.env.NEO4J_USER, process.env.NEO4J_PASSWORD)
)

const neoSchema = new Neo4jGraphQL({ typeDefs, driver });

neoSchema.getSchema().then((schema) => {
    const server = new ApolloServer({
        schema: schema
    });

    server.listen(1337).then(({ url }) => {
        console.log(`GraphQL server ready on ${url}`);
    });
});