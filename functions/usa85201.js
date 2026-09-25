export async function onRequest({ request }) {
  const url = new URL(request.url);
  url.protocol = "http:";
  url.hostname = "origin.undoab.men";
  url.port = "80";
  return fetch(new Request(url.toString(), request));
}
