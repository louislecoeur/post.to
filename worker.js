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
        return new Response("Missing hash", { status: 400 });
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
    if (pathname === "/pull" && request.method === "GET") {
      const id = url.searchParams.get("id");
      if (!id) {
        return new Response("Missing id", { status: 400 });
      }
      
      try {
        // Retrieve the binary data and its metadata from KV.
        const { value, metadata } = await env.url_kv.getWithMetadata("k_" + id, { type: "arrayBuffer" });
        
        if (value === null) {
          return new Response("Not found", { status: 404 });
        }
        
        // Extract the stored etag from metadata.
        const storedEtag = metadata && metadata.etag;
        // Get the client's If-None-Match header.
        const clientEtag = request.headers.get("if-none-match");
        
        // If the etags match, return a 304 Not Modified.
        if (storedEtag && clientEtag && storedEtag === clientEtag) {
          return new Response(null, { status: 304 });
        }
        
        // Build response headers.
        const headers = new Headers();
        headers.set("Content-Type", "application/octet-stream");
        headers.set("Content-Encoding", "identity");
        headers.set("Cache-Control", "no-transform");
        if (storedEtag) {
          headers.set("ETag", storedEtag);
        }
        
        return new Response(value, {
          status: 200,
          headers
        });
      } catch (e) {
        return new Response(e.message, { status: 500 });
      }
    }
 
    return new Response("Not found", { status: 404 });
  }

};
