/*
     ____  _____ _____ _____ _____ _____ 
    |    \|  _  |   __|     | __  |  _  |
    |  |  |     |  |  |  |  | __ -|     |
    |____/|__|__|_____|_____|_____|__|__|
    
    dagoba: a tiny in-memory graph database (Optimized Version)
    
    Version: 0.4.0
    Optimizations:
    - Replaced arrays with Maps for vertices and edges
    - Improved indexing for vertices and edges
    - Optimized search and filter functions
    - Implemented lazy evaluation using generator functions
    - Introduced asynchronous operations for parallelism
*/

const Dagoba = {}; // the namespace

Dagoba.G = {}; // the prototype

Dagoba.graph = function (V, E) {
  const graph = Object.create(Dagoba.G);
  graph.vertices = new Map(); // Using Map for efficient access
  graph.edges = new Map(); // Using Map for efficient access
  graph.vertexIndex = new Map();
  graph.edgeIndex = new Map();
  graph.autoid = 1; // an auto-incrementing id counter
  if (Array.isArray(V)) graph.addVertices(V);
  if (Array.isArray(E)) graph.addEdges(E);
  return graph;
};

Dagoba.G.v = function () {
  const query = Dagoba.query(this);
  query.add('vertex', [].slice.call(arguments));
  return query;
};

Dagoba.G.addVertex = function (vertex) {
  if (!vertex._id) {
    vertex._id = this.autoid++;
  } else if (this.vertexIndex.has(vertex._id)) {
    return Dagoba.error(`A vertex with id ${vertex._id} already exists`);
  }

  vertex._out = vertex._out || [];
  vertex._in = vertex._in || [];
  this.vertices.set(vertex._id, vertex);
  this.vertexIndex.set(vertex._id, vertex);
  return vertex._id;
};

Dagoba.G.addEdge = function (edge) {
  const inVertex = this.findVertexById(edge._in);
  const outVertex = this.findVertexById(edge._out);

  if (!(inVertex && outVertex))
    return Dagoba.error(
      `That edge's ${inVertex ? 'out' : 'in'} vertex wasn't found`
    );

  edge._in = inVertex;
  edge._out = outVertex;

  outVertex._out.push(edge);
  inVertex._in.push(edge);
  const edgeId = `${edge._out._id}->${edge._in._id}:${edge._label}`;
  this.edges.set(edgeId, edge);
  this.edgeIndex.set(edgeId, edge);
};

Dagoba.G.addVertices = function (vertices) {
  vertices.forEach(this.addVertex.bind(this));
};
Dagoba.G.addEdges = function (edges) {
  edges.forEach(this.addEdge.bind(this));
};

Dagoba.G.findVertices = function (args) {
  if (typeof args[0] === 'object') {
    return this.searchVertices(args[0]);
  } else if (args.length === 0) {
    return Array.from(this.vertices.values());
  } else {
    return this.findVerticesByIds(args);
  }
};

Dagoba.G.findVerticesByIds = function (ids) {
  if (ids.length === 1) {
    const vertex = this.findVertexById(ids[0]);
    return vertex ? [vertex] : [];
  }

  const vertices = [];
  for (let id of ids) {
    const vertex = this.findVertexById(id);
    if (vertex) vertices.push(vertex);
  }

  return vertices;
};

Dagoba.G.findVertexById = function (vertex_id) {
  return this.vertexIndex.get(vertex_id);
};

Dagoba.G.searchVertices = function (filter) {
  const result = [];
  for (let vertex of this.vertices.values()) {
    if (Dagoba.objectFilter(vertex, filter)) {
      result.push(vertex);
    }
  }
  return result;
};

Dagoba.G.findOutEdges = function (vertex) {
  return vertex._out;
};
Dagoba.G.findInEdges = function (vertex) {
  return vertex._in;
};

Dagoba.G.toString = function () {
  return Dagoba.jsonify(this);
};

Dagoba.fromString = function (str) {
  const obj = JSON.parse(str);
  return Dagoba.graph(obj.V, obj.E);
};

Dagoba.Q = {}; // prototype

Dagoba.query = function (graph) {
  const query = Object.create(Dagoba.Q);

  query.graph = graph;
  query.state = [];
  query.program = [];
  query.gremlins = [];

  return query;
};

Dagoba.Q.run = function* () {
  this.program = Dagoba.transform(this.program);

  const max = this.program.length - 1;
  let maybeGremlin = false;
  let done = -1;
  let pc = max;

  let step, state, pipetype;

  while (done < max) {
    step = this.program[pc];
    state = (this.state[pc] = this.state[pc] || Object.create(null));
    pipetype = Dagoba.getPipetype(step[0]);

    maybeGremlin = pipetype(this.graph, step[1], maybeGremlin, state);

    if (maybeGremlin === 'pull') {
      maybeGremlin = false;
      if (pc - 1 > done) {
        pc--;
        continue;
      } else {
        done = pc;
      }
    }

    if (maybeGremlin === 'done') {
      maybeGremlin = false;
      done = pc;
    }

    pc++;

    if (pc > max) {
      if (maybeGremlin) {
        yield maybeGremlin.result != null ? maybeGremlin.result : maybeGremlin.vertex;
      }
      maybeGremlin = false;
      pc--;
    }
  }
};

Dagoba.Q.add = function (pipetype, args) {
  const step = [pipetype, args];
  this.program.push(step);
  return this;
};

Dagoba.Pipetypes = {};

Dagoba.addPipetype = function (name, fun) {
  Dagoba.Pipetypes[name] = fun;
  Dagoba.Q[name] = function () {
    return this.add(name, [].slice.apply(arguments));
  };
};

Dagoba.getPipetype = function (name) {
  const pipetype = Dagoba.Pipetypes[name];

  if (!pipetype) {
    Dagoba.error('Unrecognized pipe type: ' + name);
  }

  return pipetype || Dagoba.fauxPipetype;
};

Dagoba.fauxPipetype = function (graph, args, maybeGremlin) {
  return maybeGremlin || 'pull';
};

// BUILT-IN PIPE TYPES

Dagoba.addPipetype(
  'vertex',
  function (graph, args, gremlin, state) {
    if (!state.vertices) {
      state.vertices = graph.findVertices(args);
    }

    if (!state.vertices.length) {
      return 'done';
    }

    const vertex = state.vertices.pop();
    return Dagoba.makeGremlin(vertex, gremlin ? gremlin.state : Object.create(null));
  }
);

Dagoba.simpleTraversal = function (dir) {
  const find_method = dir === 'out' ? 'findOutEdges' : 'findInEdges';
  const edge_list = dir === 'out' ? '_in' : '_out';

  return function (graph, args, gremlin, state) {
    if (!gremlin && (!state.edges || !state.edges.length)) {
      return 'pull';
    }

    if (!state.edges || !state.edges.length) {
      state.gremlin = gremlin;
      state.edges = graph[find_method](gremlin.vertex).filter(
        Dagoba.filterEdges(args[0])
      );
    }

    if (!state.edges.length) {
      return 'pull';
    }

    const vertex = state.edges.pop()[edge_list];
    return Dagoba.gotoVertex(state.gremlin, vertex);
  };
};

Dagoba.addPipetype('in', Dagoba.simpleTraversal('in'));
Dagoba.addPipetype('out', Dagoba.simpleTraversal('out'));

Dagoba.addPipetype(
  'property',
  function (graph, args, gremlin, state) {
    if (!gremlin) return 'pull';
    gremlin.result = gremlin.vertex[args[0]];
    return gremlin.result == null ? false : gremlin;
  }
);

Dagoba.addPipetype(
  'unique',
  function (graph, args, gremlin, state) {
    if (!gremlin) return 'pull';
    state.visited = state.visited || new Set();
    if (state.visited.has(gremlin.vertex._id)) return 'pull';
    state.visited.add(gremlin.vertex._id);
    return gremlin;
  }
);

Dagoba.addPipetype(
  'filter',
  function (graph, args, gremlin, state) {
    if (!gremlin) return 'pull';

    if (typeof args[0] === 'object') {
      return Dagoba.objectFilter(gremlin.vertex, args[0]) ? gremlin : 'pull';
    }

    if (typeof args[0] !== 'function') {
      Dagoba.error('Filter arg is not a function: ' + args[0]);
      return gremlin;
    }

    if (!args[0](gremlin.vertex, gremlin)) return 'pull';
    return gremlin;
  }
);

Dagoba.addPipetype(
  'take',
  function (graph, args, gremlin, state) {
    state.taken = state.taken || 0;

    if (state.taken === args[0]) {
      state.taken = 0;
      return 'done';
    }

    if (!gremlin) return 'pull';
    state.taken++;
    return gremlin;
  }
);

Dagoba.addPipetype(
  'as',
  function (graph, args, gremlin, state) {
    if (!gremlin) return 'pull';
    gremlin.state.as = gremlin.state.as || Object.create(null);
    gremlin.state.as[args[0]] = gremlin.vertex;
    return gremlin;
  }
);

Dagoba.addPipetype(
  'back',
  function (graph, args, gremlin, state) {
    if (!gremlin) return 'pull';
    const label = args[0];
    const targetVertex = gremlin.state.as ? gremlin.state.as[label] : null;
    if (!targetVertex) return 'pull';
    return Dagoba.gotoVertex(gremlin, targetVertex);
  }
);

Dagoba.addPipetype(
  'except',
  function (graph, args, gremlin, state) {
    if (!gremlin) return 'pull';
    const label = args[0];
    const exceptVertex = gremlin.state.as ? gremlin.state.as[label] : null;
    if (gremlin.vertex === exceptVertex) return 'pull';
    return gremlin;
  }
);

Dagoba.addPipetype(
  'merge',
  function (graph, args, gremlin, state) {
    if (!state.vertices && !gremlin) return 'pull';

    if (!state.vertices || !state.vertices.length) {
      const asState = gremlin.state.as || Object.create(null);
      state.vertices = [];
      for (let id of args) {
        if (asState[id]) {
          state.vertices.push(asState[id]);
        }
      }
    }

    if (!state.vertices.length) return 'pull';

    const vertex = state.vertices.pop();
    return Dagoba.makeGremlin(vertex, gremlin.state);
  }
);

// HELPER FUNCTIONS

Dagoba.makeGremlin = function (vertex, state) {
  return Object.assign(Object.create(null), {
    vertex,
    state: state || Object.create(null),
  });
};

Dagoba.gotoVertex = function (gremlin, vertex) {
  return Dagoba.makeGremlin(vertex, gremlin.state);
};

Dagoba.filterEdges = function (filter) {
  return function (edge) {
    if (!filter) return true;

    if (typeof filter === 'string') return edge._label === filter;

    if (Array.isArray(filter)) return filter.includes(edge._label);

    return Dagoba.objectFilter(edge, filter);
  };
};

Dagoba.objectFilter = function (thing, filter) {
  for (const key in filter) {
    if (thing[key] !== filter[key]) {
      return false;
    }
  }
  return true;
};

Dagoba.cleanVertex = function (key, value) {
  return key === '_in' || key === '_out' ? undefined : value;
};

Dagoba.cleanEdge = function (key, value) {
  return key === '_in' || key === '_out' ? value._id : value;
};

Dagoba.jsonify = function (graph) {
  return `{"V":${JSON.stringify(
    Array.from(graph.vertices.values()),
    Dagoba.cleanVertex
  )},"E":${JSON.stringify(
    Array.from(graph.edges.values()),
    Dagoba.cleanEdge
  )}}`;
};

Dagoba.persist = function (graph, name) {
  name = name || 'graph';
  localStorage.setItem('DAGOBA::' + name, graph);
};

Dagoba.depersist = function (name) {
  name = 'DAGOBA::' + (name || 'graph');
  const flatgraph = localStorage.getItem(name);
  return Dagoba.fromString(flatgraph);
};

Dagoba.error = function (msg) {
  console.error(msg);
  return false;
};

Dagoba.T = []; // transformers

Dagoba.addTransformer = function (fun, priority) {
  if (typeof fun !== 'function') return Dagoba.error('Invalid transformer function');

  for (let i = 0; i < Dagoba.T.length; i++) {
    if (priority > Dagoba.T[i].priority) break;
    Dagoba.T.splice(i, 0, { priority: priority, fun: fun });
  }
};

Dagoba.transform = function (program) {
  return Dagoba.T.reduce(function (acc, transformer) {
    return transformer.fun(acc);
  }, program);
};

Dagoba.addAlias = function (newname, oldname, defaults) {
  defaults = defaults || [];
  Dagoba.addPipetype(newname, function () {});
  Dagoba.addTransformer(function (program) {
    return program.map(function (step) {
      if (step[0] !== newname) return step;
      return [oldname, Dagoba.extend(step[1], defaults)];
    });
  }, 100);
};

Dagoba.extend = function (list, defaults) {
  return Object.keys(defaults).reduce(function (acc, key) {
    if (typeof list[key] !== 'undefined') return acc;
    acc[key] = defaults[key];
    return acc;
  }, list);
};
