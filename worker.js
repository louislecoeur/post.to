export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const pathname = url.pathname;

    // Handle POST /push?id=xxx&etag=yyy
    if (pathname === "/push" && request.method === "POST") {
      const id = url.searchParams.get("id");
      // Get the etag from the URL (sent from the client)
      const etag = url.searchParams.get("h");
      if (!id) {
        return new Response("Missing id", { status: 400 });
      }
      if (!etag) {
        return new Response("Missing etag", { status: 400 });
      }
      try {
        // Read the POST body as binary data.
        const data = await request.arrayBuffer();
        // Process the data (here it's simply passed through; you might compress or modify it)
        const processedData = data;
        // Save to KV under a key using the id and store the etag in metadata.
        await env.url_kv.put("k_" + id, processedData, {
          metadata: { etag: etag }
        });
        return new Response("OK", { status: 200 });
      } catch (e) {
        return new Response("Error: " + e.toString(), { status: 500 });
      }
    }

    // Handle GET /pull?id=xxx
    else if (pathname === "/pull" && request.method === "GET") {
      const id = url.searchParams.get("id");
      if (!id) {
        return new Response("Missing id", { status: 400 });
      }
      // Retrieve the binary data along with its metadata from KV.
      const { result, metadata } = await env.url_kv.getWithMetadata("k_" + id, "arrayBuffer");
      if (result === null) {
        return new Response("Not found", { status: 404 });
      }

      // Get the stored etag from metadata.
      const storedEtag = (metadata && metadata.etag);

      // Retrieve the client's If-None-Match header.
      const ifNoneMatch = request.headers.get("if-none-match");

      // Compare the stored etag with the client's etag.
      if (storedEtag && ifNoneMatch && storedEtag === ifNoneMatch) {
        return new Response(null, { status: 304 });
      }

      // Build response headers.
      const responseHeaders = {
        "Content-Type": "application/octet-stream",
        "Content-Encoding": "identity",
        "Cache-Control": "no-transform"
      };

      if (storedEtag) {
        responseHeaders["ETag"] = '"' + storedEtag + '"';
      }

      return new Response(result, {
        status: 200,
        headers: responseHeaders
      });
    }

    return new Response("Not found", { status: 404 });
  }
};
