type QueryParameterType = {
  type: string;
  arrayType?: QueryParameterType;
  structTypes?: [
    {
      name?: string;
      type: QueryParameterType;
      description?: string;
    },
  ];
  rangeElementType?: QueryParameterType;
};

type TableFieldSchema = {
  name: string;
  type: string;
  mode?: string;
  fields?: TableFieldSchema[];
  description?: string;
  policyTags?: {
    names: string[];
  };
  dataPolicies?: [
    {
      name: string;
    },
  ];
  maxLength?: string;
  precision?: string;
  scale?: string;
  roundingMode?:
    | "ROUNDING_MODE_UNSPECIFIED"
    | "ROUND_HALF_AWAY_FROM_ZERO"
    | "ROUND_HALF_EVEN";
  collation?: string;
  defaultValueExpression?: string;
  rangeElementType?: {
    type: string;
  };
};

type QueryRequest = {
  kind: string;
  query: string;
  maxResults: number;
  defaultDataset: {
    projectId: string;
    datasetId: string;
  };
  timeoutMs: number;
  destinationEncryptionConfiguration: {
    kmsKeyName: string;
  };
  dryRun: boolean;
  preserveNulls: boolean;
  useQueryCache: boolean;
  useLegacySql: boolean;
  parameterMode: string;
  queryParameters: [QueryParameterType];
  location: string;
  formatOptions: {
    useInt64Timestamp: boolean;
  };
  connectionProperties: [
    {
      key: string;
      value: string;
    },
  ];
  labels: Record<string, string>;
  maximumBytesBilled: string;
  requestId: string;
  createSession: boolean;
  jobCreationMode:
    | "JOB_CREATION_MODE_UNSPECIFIED"
    | "JOB_CREATION_REQUIRED"
    | "JOB_CREATION_OPTIONAL";
  jobTimeoutMs: string;
  reservation: string;
};

type QueryResponse = {
  kind: string;
  schema: {
    fields: TableFieldSchema[];
  };
  jobReference: {
    projectId: string;
    jobId: string;
    location: string;
  };
  jobCreationReason?: {
    code:
      | "CODE_UNSPECIFIED"
      | "REQUESTED"
      | "LONG_RUNNING"
      | "LARGE_RESULTS"
      | "OTHER";
  };
  queryId: string;
  location: string;
  totalRows: string;
  pageToken: string;
  rows: Record<string, any>[];
  totalBytesProcessed: string;
  jobComplete: boolean;
  errors: {
    reason: string;
    location: string;
    debugInfo: string;
    message: string;
  }[];
  cacheHit: boolean;
  numDmlAffectedRows: string;
  sessionInfo: {
    sessionId: string;
  };
  dmlStats: {
    insertedRowCount: string;
    deletedRowCount: string;
    updatedRowCount: string;
  };
  totalBytesBilled: string;
  totalSlotMs: string;
  creationTime: string;
  startTime: string;
  endTime: string;
};

export { type QueryRequest, type QueryResponse, type TableFieldSchema };
