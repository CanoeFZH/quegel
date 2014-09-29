#include <vector>
#include <string>
#include <fstream>
#include <cassert>

#include <graphlab.hpp>


const int inf = 1e9;

typedef int vertex_data;
typedef graphlab::empty edge_data;
typedef graphlab::distributed_graph<vertex_data, edge_data> graph_type;


const int QUERY_SIZE = 6;

const int SOURCE_LIST[QUERY_SIZE] = {0, 1, 2, 3, 4, 5}, DEST_LIST[QUERY_SIZE] = {5, 4, 3, 2, 1, 0};

int SOURCE_VERTEX, DEST_VERTEX;

void init_vertex(graph_type::vertex_type& vertex)
{
    vertex.data() = inf; 
}

struct UndirectedBFS_type : graphlab::IS_POD_TYPE {
    int dist;
    UndirectedBFS_type(int dist = inf) : dist(dist) { }
    UndirectedBFS_type& operator+=(const UndirectedBFS_type& other) {
        dist = std::min(dist, other.dist);
        return *this;
    }
};

// gather type is graphlab::empty, then we use message model
class UndirectedBFS : public graphlab::ivertex_program<graph_type, graphlab::empty, UndirectedBFS_type>,
    public graphlab::IS_POD_TYPE
{
    int min_dist;
    bool changed;
    public:
    void init(icontext_type& context, const vertex_type& vertex,
            const UndirectedBFS_type& msg) {
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
        const UndirectedBFS_type msg(newd);
        if(edge.target().data() > newd)
        {
            context.signal(edge.target(), msg);
        }
    }
};

struct UndirectedBFS_writer {
    std::string save_vertex(const graph_type::vertex_type& vtx)
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
    std::string save_edge(graph_type::edge_type e)
    {
        return "";
    }
};

bool line_parser(graph_type& graph, const std::string& filename,
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

int main(int argc, char** argv)
{
    graphlab::mpi_tools::init(argc, argv);

    char* input_file = argv[1];
    char* output_file = argv[2];
    
    std::string exec_type = "synchronous";
    graphlab::distributed_control dc;
    global_logger().set_log_level(LOG_INFO);

    graphlab::timer t;
    t.start();
    
    graph_type graph(dc);
    graph.load(input_file, line_parser);
    graph.finalize();


    dc.cout() << "Loading graph in " << t.current_time() << " seconds." << std::endl;

    double t_compute = 0, t_dump = 0;
    for(int i = 0 ;i < QUERY_SIZE; i ++)
    {
        SOURCE_VERTEX = SOURCE_LIST[i];
        DEST_VERTEX = DEST_LIST[i];
        
        t.start();

        graph.transform_vertices(init_vertex);
        graphlab::omni_engine<UndirectedBFS> engine(dc, graph, exec_type);
        engine.signal(SOURCE_VERTEX, UndirectedBFS_type(0));
        engine.start();

        t_compute +=  t.current_time();

        t.start();

        std::stringstream srtm;
        srtm << output_file << "_" << i;
        graph.save(srtm.str().c_str(), UndirectedBFS_writer(), false, // set to true if each output file is to be gzipped
            true, // whether vertices are saved
            false); // whether edges are saved
    
        t_dump += t.current_time();
    }

    dc.cout() << "Finished Running engine in " << t_compute << " seconds." << std::endl;
    dc.cout() << "Dumping graph in " << t_dump << " seconds."   << std::endl;

    graphlab::mpi_tools::finalize();
}
