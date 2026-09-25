export async function onRequest({ request }) {
  const url = new URL(request.url);
  url.protocol = "http:";
  url.hostname = "origin.undoab.men";
  url.port = "80";

  const upstream = new Request(url.toString(), request);
  upstream.headers.set("Host", "aocdisn.pages.dev");

  return fetch(upstream);
}
