export async function onRequest({ request }) {
  const url = new URL(request.url);
  url.protocol = "http:";
  url.hostname = "usa01.undoab.men";
  url.port = "80";

  return fetch(new Request(url.toString(), request));
}
