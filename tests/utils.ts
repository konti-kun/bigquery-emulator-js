import { BigQuery } from "@google-cloud/bigquery";
import { util } from "@google-cloud/common";

export const getBigQueryClient = () => {
  const options = {
    projectId: "dummy-project",
    apiEndpoint: "http://localhost:9050",
    baseUrl: "http://localhost:9050",
    scopes: ["https://www.googleapis.com/auth/bigquery"],
    packageJson: require("@google-cloud/bigquery/package.json"),
    customEndpoint: true,
  };
  const bigQuery = new BigQuery(options);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  bigQuery.makeAuthenticatedRequest = util.makeAuthenticatedRequestFactory(
    options
  ) as any;
  return bigQuery;
};
