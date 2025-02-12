import * as React from "react";
import gql from "graphql-tag";
import Loading from "../Loading";
import { useQuery } from "react-apollo";
import TypeExplorer from "./TypeExplorer";
import {
  TypeExplorerContainerQuery,
  TypeExplorerContainerQueryVariables
} from "./types/TypeExplorerContainerQuery";

interface ITypeExplorerContainerProps {
  pipelineName: string;
  typeName: string;
}

export const TypeExplorerContainer: React.FunctionComponent<
  ITypeExplorerContainerProps
> = ({ pipelineName, typeName }) => {
  const queryResult = useQuery<
    TypeExplorerContainerQuery,
    TypeExplorerContainerQueryVariables
  >(TYPE_EXPLORER_CONTAINER_QUERY, {
    fetchPolicy: "cache-and-network",
    variables: {
      pipelineName: pipelineName,
      runtimeTypeName: typeName
    }
  });
  return (
    <Loading queryResult={queryResult}>
      {data => {
        if (
          data.runtimeTypeOrError &&
          data.runtimeTypeOrError.__typename === "RegularRuntimeType"
        ) {
          return <TypeExplorer type={data.runtimeTypeOrError} />;
        } else {
          return <div>Type Not Found</div>;
        }
      }}
    </Loading>
  );
};

export const TYPE_EXPLORER_CONTAINER_QUERY = gql`
  query TypeExplorerContainerQuery(
    $pipelineName: String!
    $runtimeTypeName: String!
  ) {
    runtimeTypeOrError(
      pipelineName: $pipelineName
      runtimeTypeName: $runtimeTypeName
    ) {
      __typename
      ... on RegularRuntimeType {
        ...TypeExplorerFragment
      }
    }
  }

  ${TypeExplorer.fragments.TypeExplorerFragment}
`;
