import { ApolloClient, HttpLink, InMemoryCache } from "@apollo/client";

export function getApolloClient(uri: string, token: string) {
    return new ApolloClient({
        link: new HttpLink({
            uri: uri,
            headers: {
            // token header for auth
            Authorization: `Bearer ${token}`,
            },
        }),
        cache: new InMemoryCache(),
    })
}


