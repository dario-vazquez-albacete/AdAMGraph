const { gql, ApolloServer } = require("apollo-server");
const { Neo4jGraphQL } = require("@neo4j/graphql");
const neo4j = require("neo4j-driver");
require("dotenv").config();

const typeDefs = gql`
type Patient {
  USUBJID: String!
  ARM: String!
  AGE: Int!
  SEX: String!
  BMI: Int!
  experiencedAdverseEventAdverseevent: [AdverseEvent!]! @relationship(type: "EXPERIENCED_ADVERSE_EVENT", direction: OUT, properties: "ExperiencedAdverseEvent")
  attendedVisitVisit: [Visit!]! @relationship(type: "ATTENDED_VISIT", direction: OUT)
  wasTreatedTreatment: [Treatment!]! @relationship(type: "WAS_TREATED", direction: OUT, properties: "WasTreated")
  measuredLabparameterParameter: [Parameter!]! @relationship(type: "MEASURED_LABPARAMETER", direction: OUT, properties: "MeasuredLabparameter")
  measuredEndpointEndpoint: [Endpoint!]! @relationship(type: "MEASURED_ENDPOINT", direction: OUT, properties: "MeasuredEndpoint")
  measuredVitalsignVitalsign: [VitalSign!]! @relationship(type: "MEASURED_VITALSIGN", direction: OUT, properties: "MeasuredVitalsign")
}

type AdverseEvent {
  Term: String!
  patientExperiencedAdverseEvent: [Patient!]! @relationship(type: "EXPERIENCED_ADVERSE_EVENT", direction: IN, properties: "ExperiencedAdverseEvent")
}

type Visit {
  Name: String!
  patientAttendedVisit: [Patient!]! @relationship(type: "ATTENDED_VISIT", direction: IN)
  measuredInVisitParameter: [Parameter!]! @relationship(type: "MEASURED_IN_VISIT", direction: OUT)
  measuredInVisitEndpoint: [Endpoint!]! @relationship(type: "MEASURED_IN_VISIT", direction: OUT)
  measuredInVisitVitalsign: [VitalSign!]! @relationship(type: "MEASURED_IN_VISIT", direction: OUT)
}

type Parameter {
  Parameter: String!
  Dataset: String!
  Value: Float!
  Reference: String!
  patientMeasuredLabparameter: [Patient!]! @relationship(type: "MEASURED_LABPARAMETER", direction: IN, properties: "MeasuredLabparameter")
  visitMeasuredInVisit: [Visit!]! @relationship(type: "MEASURED_IN_VISIT", direction: IN)
}

type Endpoint {
  Name: String!
  Dataset: String!
  patientMeasuredEndpoint: [Patient!]! @relationship(type: "MEASURED_ENDPOINT", direction: IN, properties: "MeasuredEndpoint")
  visitMeasuredInVisit: [Visit!]! @relationship(type: "MEASURED_IN_VISIT", direction: IN)
}

type Treatment {
  Name: String!
  patientWasTreated: [Patient!]! @relationship(type: "WAS_TREATED", direction: IN, properties: "WasTreated")
}

type VitalSign {
  Parameter: String!
  Value: Int!
  Dataset: String!
  patientMeasuredVitalsign: [Patient!]! @relationship(type: "MEASURED_VITALSIGN", direction: IN, properties: "MeasuredVitalsign")
  visitMeasuredInVisit: [Visit!]! @relationship(type: "MEASURED_IN_VISIT", direction: IN)
}

interface ExperiencedAdverseEvent {
  Severity: Int
  Type: String
}

interface WasTreated {
  Dose: Int
}

interface MeasuredLabparameter {
  ChangeFromBaseline: Int
}

interface MeasuredEndpoint {
  ChangeFromBaseline: Int
}

interface MeasuredVitalsign {
  ChangeFromBaseline: Int
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