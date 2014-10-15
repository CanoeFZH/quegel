#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cmath>
#include <algorithm>

#include <vector>
using namespace std;

const int dx[] = { 0,  1,  0,  -1 };
const int dy[] = { -1,  0,  1,  0 };

int global_id = 0;
const double DIST = 5;
const double equatorial = 6378.1370;
const double polar = 6356.7523;
const double PI = acos(-1.0);
double getEarthRadius(double lat)
{
	double Cos = cos(lat);
	double Cos1 = equatorial*Cos;
	double Cos2 = equatorial*Cos1;
	double Sin = sin(lat);
	double Sin1 = polar*Sin;
	double Sin2 = polar*Sin1;
	double n1 = Cos2*Cos2 + Sin2*Sin2;
	double n2 = Cos1*Cos1 + Sin1*Sin1;
	return 1000 * sqrt(n1 / n2);//km->m
}


double getGPSDist(double lat1, double lon1, double lat2, double lon2)//µ¥Î»m
{//http://andrew.hedges.name/experiments/haversine/
	double lat_1 = lat1*PI / 180;
	double lon_1 = lon1*PI / 180;
	double lat_2 = lat2*PI / 180;
	double lon_2 = lon2*PI / 180;
	double dlat = lat_2 - lat_1;
	double dlon = lon_2 - lon_1;
	double s1 = (1 - cos(dlat)) / 2;
	double s2 = (1 - cos(dlon)) / 2;
	double a = s1 + cos(lat_1)*cos(lat_2)*s2;
	double c = 2 * atan2(sqrt(a), sqrt(1 - a));
	double mlat = (lat_1 + lat_2) / 2;
	return getEarthRadius(mlat)*c;
}

double get3DDist(double x1, double y1, double z1, double x2, double y2, double z2)
{
	/*
	double xydis = getGPSDist( y1,x1, y2, x2);
	int dz = z1 - z2;
	return sqrt( xydis * xydis  +  dz * dz );
	*
	*/

	return sqrt((x1 - x2) *(x1 - x2) + (y1 - y2) * (y1 - y2) + (z1 - z2) *(z1 - z2));
}

struct Point
{
	double x, y, z;
	int id;
	vector<Point*> adj;
	Point(){}
	Point(double _x, double _y, double _z, int _id)
	{
		x = _x;
		y = _y;
		z = _z;
		id = _id;
	}
};

struct Grid
{
	/*
		p0 ---E0--- p1
		|			|
		E1			E2
		|			|
		p2 ---E3--- p3
	
	*/

	Point *p0, *p1, *p2, *p3;
	vector<Point*> *e0, *e1, *e2, *e3;

	Grid()
	{
		p0 = p1 = p2 = p3 = 0;
		e0 = e1 = e2 = e3 = 0;
	}
}; 

int ncols, nrows;
double NODATA_value;
double xllcorner, yllcorner, cellsize;
vector< vector<Point> > points;
vector< vector<Grid> > grids;

double getX(int j)
{
	return xllcorner + j * cellsize;
}
double getY(int i)
{
	return yllcorner + cellsize * (nrows - 1) - i * cellsize;
}

bool exist(int i, int j)
{
	return i >= 0 && i < nrows && j >= 0 && j < ncols;
	
}

bool equal(double a, double b)
{
	return fabs(a - b) < 1e-8;
}


void connect(Point* p0, Point* p1)
{
	if (p0->id == -1 || p1->id == -1) return;
	p0->adj.push_back(p1);
	p1->adj.push_back(p0);
}

void add0(Point* p, vector<Point*>* e)
{
	if (e == 0) return;
	vector<Point*>& ve = *e;
	for (int i = 0; i < ve.size(); i++)
	{
		connect(p, ve[i]);
	}
}

void add(Point* p, vector<Point*>* e0, vector<Point*>* e1)
{
	if (p->id == -1) return;
	add0(p, e0);
	add0(p, e1);
}

void add(Point* p0, Point* p1 , vector<Point*>*& e)
{
	if (p0->id == -1 || p1->id == -1) return;
	double dist = get3DDist(p0->x, p0->y, p0->z, p1->x, p1->y, p1->z);
	if (dist <= DIST)
	{
		connect(p0, p1);
		return;
	}
	e = new vector<Point*>();

	double ddx = p1->x - p0->x, ddy = p1->y - p0->y, ddz = p1->z - p0->z;

	int numToAdd = (int) (ceil(dist / DIST))  - 1; 

	/*
	
		dist = 10, DIST = 5, add 1 pt
		dist = 10, DIST = 4, add 2 pt
	
	*/

	double deltax = ddx / (numToAdd + 1);
	double deltay = ddy / (numToAdd + 1);
	double deltaz = ddz / (numToAdd + 1);

	for (int i = 1; i <= numToAdd; i++)
	{
		Point* p = new Point(p0->x + deltax * i, p0->y + deltay * i, p0->z + deltaz * i, global_id++);
		e->push_back(p);

		if (i >= 2)
		{
			connect(p, *(e->end() - 2));
		}
	}

	connect(p0, *e->begin());
	connect(p1, e->back());
}

void link0(vector<Point*>*& e, vector<Point*>* e0)
{
	if (e0 == 0) return;

	vector<Point*>& ve = *e;
	vector<Point*>& ve0 = *e0;

	for (int i = 0; i < ve.size(); i++)
	{
		for (int j = 0; j < ve0.size(); j++)
		{
			ve[i]->adj.push_back(ve0[j]);
		}
	}
}

void link(vector<Point*>*& e, vector<Point*>* e1, vector<Point*>* e2, vector<Point*>* e3)
{
	if (e == 0) return;
	link0(e, e1);
	link0(e, e2);
	link0(e, e3);
} 


void outputPoint(Point& p, FILE* fout)
{
	if (p.id == -1) return;
	vector<Point*>& adj = p.adj;
	fprintf(fout, "%d %lf %lf %lf\t%d", p.id, p.x, p.y, p.z, (int)(adj.size()));

	for (int i = 0; i < adj.size(); i++)
	{
		double dis = get3DDist(p.x, p.y, p.z, adj[i]->x, adj[i]->y, adj[i]->z);
		fprintf(fout, " %d %lf", adj[i]->id, dis);
	}
	fprintf(fout, "\n");
}

void outputEdge(vector<Point*>* e, FILE* fout)
{
	if (e == 0) return;
	vector<Point*>& ve = *e;
	for (int i = 0; i < ve.size(); i++)
	{
		outputPoint(*ve[i], fout);
	}
}


int main(int argc, char* argv[])
{
	/* parse meta data  */

	FILE* fin = fopen(argv[1], "r");
	FILE* fout = fopen(argv[2], "w");

	fscanf(fin, "%*s %d", &ncols);
	fscanf(fin, "%*s %d", &nrows);
	fscanf(fin, "%*s %lf", &xllcorner);
	fscanf(fin, "%*s %lf", &yllcorner);
	fscanf(fin, "%*s %lf", &cellsize);
	fscanf(fin, "%*s %lf", &NODATA_value);

	printf("%d %d %lf %lf %lf %lf\n", ncols, nrows, xllcorner, yllcorner, cellsize, NODATA_value);

	// http://en.wikipedia.org/wiki/Esri_grid

	
	for (int i = 0; i < nrows; i++)
	{
		points.push_back(vector<Point>());
		for (int j = 0; j < ncols; j++)
		{
			double z;
			fscanf(fin, "%lf", &z);
			double x = getX(j), y = getY(i);
			points[i].push_back(Point(x, y, z, equal(z, NODATA_value) ? -1 : global_id++));
			
		}
	}

	// build grid

	for (int i = 0; i < nrows - 1; i++)
	{
		grids.push_back(vector<Grid>());
		grids[i].resize(ncols - 1);
		for (int j = 0; j < ncols - 1; j++)
		{
			grids[i][j].p0 = &points[i][j];
			grids[i][j].p1 = &points[i][j+1];
			grids[i][j].p2 = &points[i+1][j];
			grids[i][j].p3 = &points[i+1][j+1];




			// E0

			if (i != 0)
				grids[i][j].e0 = grids[i - 1][j].e3;
			else
				add(grids[i][j].p0, grids[i][j].p1, grids[i][j].e0);
				

			// E1

			if (j != 0)
				grids[i][j].e1 = grids[i][j - 1].e2;
			else
				add(grids[i][j].p0, grids[i][j].p2, grids[i][j].e1);

			// E2

			add(grids[i][j].p1, grids[i][j].p3, grids[i][j].e2);

			// E3

			add(grids[i][j].p2, grids[i][j].p3, grids[i][j].e3);


			// link p0 ~ 03 to e0 ~ e3
			add(grids[i][j].p0, grids[i][j].e2, grids[i][j].e3);
			add(grids[i][j].p1, grids[i][j].e1, grids[i][j].e3);
			add(grids[i][j].p2, grids[i][j].e0, grids[i][j].e2);
			add(grids[i][j].p3, grids[i][j].e0, grids[i][j].e1);

			link(grids[i][j].e0, grids[i][j].e1, grids[i][j].e2, grids[i][j].e3);
			link(grids[i][j].e1, grids[i][j].e0, grids[i][j].e2, grids[i][j].e3);
			link(grids[i][j].e2, grids[i][j].e0, grids[i][j].e1, grids[i][j].e3);
			link(grids[i][j].e3, grids[i][j].e0, grids[i][j].e1, grids[i][j].e2);


			connect(grids[i][j].p0, grids[i][j].p3);
			connect(grids[i][j].p1, grids[i][j].p2);
		}
	}


	for (int i = 0; i < nrows; i++)
	{
		for (int j = 0; j < ncols; j++)
		{
			outputPoint(points[i][j],fout);
		}
	}

	for (int i = 0; i < nrows - 1; i++)
	{
		for (int j = 0; j < ncols - 1; j++)
		{
			if (i == 0)
				outputEdge(grids[i][j].e0, fout);
			if (j == 0)
				outputEdge(grids[i][j].e1, fout);
			outputEdge(grids[i][j].e2, fout);
			outputEdge(grids[i][j].e3, fout);
		}
	}

	fclose(fout);
	fclose(fin);
	return 0;
}
