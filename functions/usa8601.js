export async function onRequest({ request }) {
  const url = new URL(request.url);
  url.protocol = "http:";
  url.hostname = "64.181.249.69";
  url.port = "80";

  return fetch(new Request(url.toString(), request));
}
