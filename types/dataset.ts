type Dataset = {
  kind: string;
  etag: string;
  id: string;
  selfLink: string;
  datasetReference: {
    projectId: string;
    datasetId: string;
  };
  friendlyName?: string;
  description?: string;
  defaultTableExpirationMs?: string;
  defaultPartitionExpirationMs?: string;
  labels?: Record<string, string>;
  access?: [
    {
      role: string;
      userByEmail: string;
      groupByEmail: string;
      domain: string;
      specialGroup: string;
      iamMember: string;
      view: {
        projectId: string;
        datasetId: string;
        tableId: string;
      };
      routine: {
        projectId: string;
        datasetId: string;
        routineId: string;
      };
      dataset: {
        dataset: {
          datasetId: string;
          projectId: string;
        };
        targetTypes: "TARGET_TYPE_UNSPECIFIED" | "VIEWS";
      };
      condition: {
        title: string;
        description?: string;
        expression?: string;
        location?: string;
      };
    },
  ];
  creationTime: string;
  lastModifiedTime: string;
  location: string;
  defaultEncryptionConfiguration: {
    kmsKeyName: string;
  };
  satisfiesPzs: boolean;
  satisfiesPzi: boolean;
  type: string;
  linkedDatasetSource?: {
    sourceDataset: {
      datasetId: string;
      projectId: string;
    };
  };
  linkedDatasetMetadata: {
    linkState: "LINK_STATE_UNSPECIFIED" | "LINKED" | "UNLINKED";
  };
  externalDatasetReference?: {
    externalSource: string;
    connection: string;
  };
  externalCatalogDatasetOptions?: {
    parameters: Record<string, string>;
    defaultStorageLocationUri: string;
  };
  isCaseInsensitive?: boolean;
  defaultCollation?: string;
  defaultRoundingMode?:
    | "ROUNDING_MODE_UNSPECIFIED"
    | "ROUND_HALF_AWAY_FROM_ZERO"
    | "ROUND_HALF_EVEN";
  maxTimeTravelHours?: string;
  tags?: [
    {
      key: string;
      value: string;
    },
  ];
  storageBillingModel?:
    | "STORAGE_BILLING_MODEL_UNSPECIFIED"
    | "LOGICAL"
    | "PHYSICAL";
  resourceTags?: Record<string, string>;
};

type PostDatasetRequest = Omit<
  Dataset,
  | "kind"
  | "etag"
  | "id"
  | "selfLink"
  | "creationTime"
  | "lastModifiedTime"
  | "location"
  | "satisfiesPzs"
  | "satisfiesPzi"
  | "type"
  | "linkedDatasetMetadata"
  | "tags"
>;

export type { Dataset, PostDatasetRequest };
