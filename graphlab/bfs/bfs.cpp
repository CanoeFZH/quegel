#include <vector>
#include <string>
#include <fstream>
#include <cassert>

#include <graphlab.hpp>
#include <cstdio>
const int inf = 1e9;

typedef int vertex_data;
typedef graphlab::empty edge_data;
typedef graphlab::distributed_graph<vertex_data, edge_data> UndirectedGraph;

struct BiVertex_data : graphlab::IS_POD_TYPE
{
    int forward;
    int backward;
};

typedef graphlab::distributed_graph<BiVertex_data, edge_data> DirectedGraph;

const int QUERY_SIZE = 10;

const int SOURCE_LIST[QUERY_SIZE] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, DEST_LIST[QUERY_SIZE] = {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};

int SOURCE_VERTEX, DEST_VERTEX;
int ForwardCover, BackwardCover;
int BiDist, BiIteration;

int KHop = 3;

void init_vertex(UndirectedGraph::vertex_type& vertex)
{
    vertex.data() = inf; 
}

void init_bivertex(DirectedGraph::vertex_type& vertex)
{
    if(vertex.id() == SOURCE_VERTEX)
        vertex.data().forward = 0;
    else
        vertex.data().forward = inf; 
    
    if(vertex.id() == DEST_VERTEX)
        vertex.data().backward = 0; 
    else
        vertex.data().backward = inf;
}

void set_dist(DirectedGraph::vertex_type& vertex)
{
    if(vertex.id() == DEST_VERTEX)
        vertex.data().forward = BiDist; 
}



size_t count_forward_cover(const DirectedGraph::vertex_type& vertex) {
        return vertex.data().forward != inf;
}

size_t count_backward_cover(const DirectedGraph::vertex_type& vertex) {
        return vertex.data().backward != inf;
}

struct min_t : public graphlab::IS_POD_TYPE {
    int value;
    min_t()
        : value(inf)
    {
    }
    min_t(int _value)
        : value(_value)
    {
    }
    min_t& operator+=(const min_t& other)
    {
        value = std::min(value, other.value);
        return *this;
    }
};

min_t get_dist(const DirectedGraph::vertex_type& vertex)
{
    return min_t(vertex.data().forward + vertex.data().backward);
}

struct SingleSourceBFS_type : graphlab::IS_POD_TYPE {
    int dist;
    SingleSourceBFS_type(int dist = inf) : dist(dist) { }
    SingleSourceBFS_type& operator+=(const SingleSourceBFS_type& other) {
        dist = std::min(dist, other.dist);
        return *this;
    }
};

typedef SingleSourceBFS_type KHop_type;

struct BiBFS_type : graphlab::IS_POD_TYPE {
    int flag;
    BiBFS_type(int flag = 0) : flag(flag) { }
    BiBFS_type& operator+=(const BiBFS_type& other) {
        flag |= other.flag;
        return *this;
    }
};


// gather type is graphlab::empty, then we use message model
class SingleSourceBFS : public graphlab::ivertex_program<UndirectedGraph, graphlab::empty, SingleSourceBFS_type>,
    public graphlab::IS_POD_TYPE
{
    int min_dist;
    bool changed;
    public:
    void init(icontext_type& context, const vertex_type& vertex,
            const SingleSourceBFS_type& msg) {
        min_dist = msg.dist;
    } 

    edge_dir_type gather_edges(icontext_type& context, 
            const vertex_type& vertex) const { 
        return graphlab::NO_EDGES;
    };


    void apply(icontext_type& context, vertex_type& vertex, const graphlab::empty& empty)
    {
        changed = false;
        if(vertex.data() > min_dist) {
            changed = true;
            vertex.data() = min_dist;
            if(vertex.id() == DEST_VERTEX)
            {
                context.stop();
            }
        }
    }

    edge_dir_type scatter_edges(icontext_type& context,
            const vertex_type& vertex) const
    {
        if (changed)
            return graphlab::OUT_EDGES;
        else
            return graphlab::NO_EDGES;
    }

    void scatter(icontext_type& context, const vertex_type& vertex,
            edge_type& edge) const
    {
        int newd = vertex.data() + 1;
        const SingleSourceBFS_type msg(newd);
        if(edge.target().data() > newd)
        {
            context.signal(edge.target(), msg);
        }
    }
};
// gather type is graphlab::empty, then we use message model
class BiBFS : public graphlab::ivertex_program<DirectedGraph, graphlab::empty, BiBFS_type>,
    public graphlab::IS_POD_TYPE
{
    int my_flag;
    public:
    void init(icontext_type& context, const vertex_type& vertex,
            const BiBFS_type& msg) {
        my_flag = msg.flag;
    } 

    edge_dir_type gather_edges(icontext_type& context, 
            const vertex_type& vertex) const { 
        return graphlab::NO_EDGES;
    };


    void apply(icontext_type& context, vertex_type& vertex, const graphlab::empty& empty)
    {
        if(context.iteration() == 0)
        {
            ; // send msgs
        }
        else // if(context.iteration() == 1)
        {
            if( vertex.data().forward == inf && (my_flag & 1) )
            {
                vertex.data().forward = BiIteration;
            }

            if( vertex.data().backward == inf && (my_flag & 2) )
            {
                vertex.data().backward = BiIteration;
            }
        }
    }

    edge_dir_type scatter_edges(icontext_type& context,
            const vertex_type& vertex) const
    {
        if(context.iteration() == 0)
        {
            if(vertex.data().forward != inf)
                return graphlab::OUT_EDGES;
            else
                return graphlab::IN_EDGES;
        }
        else
        {
            return graphlab::NO_EDGES;
        }
    }

    void scatter(icontext_type& context, const vertex_type& vertex,
            edge_type& edge) const
    {
        if(edge.source().id() == vertex.id()) // OUT_EDGE
        {
            const BiBFS_type msg(1);
            if (edge.target().data().forward == inf)
            {
                context.signal(edge.target(), msg);
            }
        }
        else
        {
            const BiBFS_type msg(2);
            if (edge.source().data().backward == inf)
            {
                context.signal(edge.source(), msg);
            }
        }
    }
};

typedef graphlab::synchronous_engine<BiBFS> engine_type;

graphlab::empty signal_vertices(engine_type::icontext_type& ctx,
        const DirectedGraph::vertex_type& vertex) {
    if (vertex.data().forward != inf || vertex.data().backward != inf) {
        ctx.signal(vertex);
    }
    return graphlab::empty();
}

// gather type is graphlab::empty, then we use message model
class KHopBFS : public graphlab::ivertex_program<UndirectedGraph, graphlab::empty, KHop_type>,
    public graphlab::IS_POD_TYPE
{
    int min_dist;
    bool changed;
    public:
    void init(icontext_type& context, const vertex_type& vertex,
            const KHop_type& msg) {
        min_dist = msg.dist;
    } 

    edge_dir_type gather_edges(icontext_type& context, 
            const vertex_type& vertex) const { 
        return graphlab::NO_EDGES;
    };


    void apply(icontext_type& context, vertex_type& vertex, const graphlab::empty& empty)
    {
        if(context.iteration() == KHop + 1)
        {
            context.stop();
            return;
        }

        changed = false;
        if(vertex.data() > min_dist) {
            changed = true;
            vertex.data() = min_dist;
        }
    }

    edge_dir_type scatter_edges(icontext_type& context,
            const vertex_type& vertex) const
    {
        if (changed && context.iteration() < KHop)
            return graphlab::OUT_EDGES;
        else
            return graphlab::NO_EDGES;
    }

    void scatter(icontext_type& context, const vertex_type& vertex,
            edge_type& edge) const
    {
        int newd = vertex.data() + 1;
        const SingleSourceBFS_type msg(newd);
        if(edge.target().data() > newd)
        {
            context.signal(edge.target(), msg);
        }
    }
};



struct SingleSourceBFS_writer {
    std::string save_vertex(const UndirectedGraph::vertex_type& vtx)
    {
        if(vtx.id() == DEST_VERTEX)
        {
            std::stringstream strm;
            strm << vtx.id() << "\t" << vtx.data() << "\n";
            return strm.str();
        }
        else
        {
            return "";
        }
    }
    std::string save_edge(UndirectedGraph::edge_type e)
    {
        return "";
    }
};

struct BiBFS_writer {
    std::string save_vertex(const DirectedGraph::vertex_type& vtx)
    {
        if(vtx.id() == DEST_VERTEX)
        {
            std::stringstream strm;
            int dist = vtx.data().forward; // dist is stored at forward
            strm << vtx.id() << "\t" << dist << "\n";
            return strm.str();
        }
        else
        {
            return "";
        }
    }
    std::string save_edge(DirectedGraph::edge_type e)
    {
        return "";
    }
};


struct KHop_writer {
    std::string save_vertex(const UndirectedGraph::vertex_type& vtx)
    {
        if(vtx.data() != inf)
        {
            std::stringstream strm;
            strm << vtx.id() << "\t" << vtx.data() << "\n";
            return strm.str();
        }
        else
        {
            return "";
        }
    }
    std::string save_edge(UndirectedGraph::edge_type e)
    {
        return "";
    }
};


bool UndirectedParser(UndirectedGraph& graph, const std::string& filename,
        const std::string& textline)
{
    std::istringstream ssin(textline);
    graphlab::vertex_id_type vid;
    ssin >> vid;
    int num;
    ssin >> num;
    for(int i = 0 ;i < num ; i ++)
    {
        graphlab::vertex_id_type other_vid;
        ssin >> other_vid;
        graph.add_edge(vid, other_vid);
    }
    return true;
}

bool UndirectedBiParser(DirectedGraph& graph, const std::string& filename,
        const std::string& textline)
{
    std::istringstream ssin(textline);
    graphlab::vertex_id_type vid;
    ssin >> vid;
    int num;
    ssin >> num;
    for(int i = 0 ;i < num ; i ++)
    {
        graphlab::vertex_id_type other_vid;
        ssin >> other_vid;
        graph.add_edge(vid, other_vid);
    }
    
    return true;
}


bool DirectedBiParser(DirectedGraph& graph, const std::string& filename,
        const std::string& textline)
{
    std::istringstream ssin(textline);
    graphlab::vertex_id_type vid;
    ssin >> vid;
    int in_num, out_num;
    ssin >> in_num;
    for(int i = 0 ;i < in_num; i ++)
    {
        graphlab::vertex_id_type other_vid;
        ssin >> other_vid; // skip in_neighbors;
    }
    ssin >> out_num;
    for(int i = 0 ;i < out_num ; i ++)
    {
        graphlab::vertex_id_type other_vid;
        ssin >> other_vid;
        graph.add_edge(vid, other_vid);
    }
    return true;
}


int main(int argc, char** argv)
{
    graphlab::mpi_tools::init(argc, argv);

    char* input_file = argv[1];
    char* output_file = argv[2];
    char* opt = argv[3]; // bfs bibfsDG bibfsUG KHop

    std::string exec_type = "synchronous";
    graphlab::distributed_control dc;
    global_logger().set_log_level(LOG_INFO);

    if(strcmp(opt, "bfs") == 0)
    {
        graphlab::timer t;
        t.start();

        UndirectedGraph graph(dc);
        graph.load(input_file, UndirectedParser);
        graph.finalize();


        dc.cout() << "Loading graph in " << t.current_time() << " seconds." << std::endl;

        double t_compute = 0, t_dump = 0;
        for(int i = 0 ;i < QUERY_SIZE; i ++)
        {
            SOURCE_VERTEX = SOURCE_LIST[i];
            DEST_VERTEX = DEST_LIST[i];

            t.start();

            graph.transform_vertices(init_vertex);
            graphlab::omni_engine<SingleSourceBFS> engine(dc, graph, exec_type);
            engine.signal(SOURCE_VERTEX, SingleSourceBFS_type(0));
            engine.start();

            t_compute +=  t.current_time();

            t.start();

            std::stringstream srtm;
            srtm << output_file << "_" << i;
            graph.save(srtm.str().c_str(), SingleSourceBFS_writer(), false, // set to true if each output file is to be gzipped
                    true, // whether vertices are saved
                    false); // whether edges are saved

            t_dump += t.current_time();
        }

        dc.cout() << "Finished Running engine in " << t_compute << " seconds." << std::endl;
        dc.cout() << "Dumping graph in " << t_dump << " seconds."   << std::endl;
    }
    else if(strcmp(opt, "bibfsDG") == 0 || strcmp(opt, "bibfsUG") == 0)
    {
        graphlab::timer t;
        t.start();

        DirectedGraph graph(dc);
        if(strcmp(opt, "bibfsUG") == 0 )
        {
            graph.load(input_file, UndirectedBiParser);
        }
        else
        {
            graph.load(input_file, DirectedBiParser);
        }
        graph.finalize();


        dc.cout() << "Loading graph in " << t.current_time() << " seconds." << std::endl;

        double t_compute = 0, t_dump = 0;
        for(int i = 0 ;i < QUERY_SIZE; i ++)
        {
            SOURCE_VERTEX = SOURCE_LIST[i];
            DEST_VERTEX = DEST_LIST[i];
            ForwardCover = 0;
            BackwardCover = 0;

            t.start();
            graph.transform_vertices(init_bivertex);

            BiIteration = 0;
            while(true)
            {
                BiIteration ++;

                graphlab::omni_engine<BiBFS> engine(dc, graph, exec_type);
                engine.map_reduce_vertices<graphlab::empty>(signal_vertices);
                engine.start();

                BiDist = graph.map_reduce_vertices<min_t>(get_dist).value;
                
                dc.cout() << "BiDist: " << BiDist << std::endl;
                
                if(BiDist != inf)
                    break;

                int fc = graph.map_reduce_vertices<size_t>(count_forward_cover);
                int bc = graph.map_reduce_vertices<size_t>(count_backward_cover);
                
                dc.cout() << "FC: " << fc << " BC: " << bc << std::endl;
                
                if(fc - ForwardCover == 0 || bc - BackwardCover == 0)
                {
                    BiDist = inf;
                    break;
                }
                else
                {
                    ForwardCover = fc;
                    BackwardCover = bc;
                }
            }
            graph.transform_vertices(set_dist);
            t_compute +=  t.current_time();
            
            t.start();

            std::stringstream srtm;
            srtm << output_file << "_" << i;
            graph.save(srtm.str().c_str(), BiBFS_writer(), false, // set to true if each output file is to be gzipped
                    true, // whether vertices are saved
                    false); // whether edges are saved

            t_dump += t.current_time();
        }

        dc.cout() << "Finished Running engine in " << t_compute << " seconds." << std::endl;
        dc.cout() << "Dumping graph in " << t_dump << " seconds."   << std::endl;
    }
    else if(strcmp(opt, "KHop") == 0)
    {
        graphlab::timer t;
        t.start();

        UndirectedGraph graph(dc);
        graph.load(input_file, UndirectedParser);
        graph.finalize();


        dc.cout() << "Loading graph in " << t.current_time() << " seconds." << std::endl;

        double t_compute = 0, t_dump = 0;
        for(int i = 0 ;i < QUERY_SIZE; i ++)
        {
            SOURCE_VERTEX = SOURCE_LIST[i];

            t.start();

            graph.transform_vertices(init_vertex);
            graphlab::omni_engine<KHopBFS> engine(dc, graph, exec_type);
            engine.signal(SOURCE_VERTEX, KHop_type(0));
            engine.start();

            t_compute +=  t.current_time();

            t.start();

            std::stringstream srtm;
            srtm << output_file << "_" << i;
            graph.save(srtm.str().c_str(), KHop_writer(), false, // set to true if each output file is to be gzipped
                    true, // whether vertices are saved
                    false); // whether edges are saved

            t_dump += t.current_time();
        }

        dc.cout() << "Finished Running engine in " << t_compute << " seconds." << std::endl;
        dc.cout() << "Dumping graph in " << t_dump << " seconds."   << std::endl;
    }
    graphlab::mpi_tools::finalize();
}
