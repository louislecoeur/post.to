export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const pathname = url.pathname;

    // Handle POST /push?id=xxx
    if (pathname === "/push" && request.method === "POST") {
      const id = url.searchParams.get("id");
      if (!id) {
        return new Response("Missing id", { status: 400 });
      }
      try {
        // Read the POST body as binary data.
        const data = await request.arrayBuffer();
        // Process the data (here it's simply passed through; you might compress or modify it)
        const processedData = data; 
        // Save to KV under a key using the id.
        await env.url_kv.put("k_" + id, processedData);
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
      // Retrieve the binary data from KV.
      const storedData = await env.url_kv.get("k_" + id, "arrayBuffer");
      if (storedData === null) {
        return new Response("Not found", { status: 404 });
      }
      return new Response(storedData, {
        status: 200,
        headers: { "Content-Type": "application/octet-stream",
                   "Content-Encoding": "identity",
                   "Cache-Control": "no-transform" }
      });
    }

    return new Response("Not found", { status: 404 });
  }
};


/*


export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const pathname = url.pathname;

    // Handle POST /push?id=xxx
    if (pathname === "/push" && request.method === "POST") {
      const id = url.searchParams.get("id");
      if (!id) {
        return new Response("Missing id", { status: 400 });
      }
      try {
        // Read the POST body as plain text (or JSON, if that's what you send).
        const data = await request.text();
        // For now, compress and hash are identity functions.
        const processedData = data;  // compress(data) in real implementation
        // Save to KV under a key using the id.
        await env.url_kv.put("content_" + id, processedData);
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
      const storedData = await env.url_kv.get("content_" + id);
      if (storedData === null) {
        return new Response("Not found", { status: 404 });
      }
      return new Response(storedData, {
        status: 200,
        headers: { "Content-Type": "text/plain" }
      });
    }

    return new Response("Not found", { status: 404 });
  }
};

*/
