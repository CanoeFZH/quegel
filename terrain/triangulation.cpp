#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cmath>
#include <algorithm>

#include <vector>
using namespace std;

// from 12 oclock direction, clockwise
const int dx[] = { 0, 1, 1, 1, 0, -1, -1, -1 };
const int dy[] = { -1, -1, 0, 1, 1, 1, 0, -1 };


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
	int id;
	double z;
	int nb;
	void setNb(int dir)
	{
		nb |= (1 << dir);
	}
	int countBit()
	{
		int size = 0, tb = nb;
		while (tb)
		{
			size++;
			tb &= (tb - 1);
		}
		return size;

	}
	Point(double _z, int _id)
	{
		z = _z;
		id = _id;
		nb = 0;
	}
};

struct Triangle
{
	int x, y, z;
	Triangle(int _x, int _y, int _z)
	{
		/*
		x = min((min(_x, _y)), _z);
		z = max((max(_x, _y)), _z);
		y = _x + _y + _z - x - z;
		*/
		x = _x;
		y = _y;
		z = _z;
	}
	/*
	bool operator == (const Triangle& t) const
	{
		return x == t.x && y == t.y && z == t.z;
	}
	bool operator < (const Triangle& t) const
	{
		if (x != t.x)
			return x < t.x;
		else
		{
			if (y != t.y)
				return y < t.y;
			else
			{
				return z < t.z;
			}
		}
	}
	*/
};


int ncols, nrows;
double NODATA_value;
double xllcorner, yllcorner, cellsize;
vector< vector<Point> > data;
vector< Triangle > vt;
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
	return i >= 0 && i < nrows && j >= 0 && j < ncols && data[i][j].id != -1;

}
double getCos(double a, double b, double c)
{
	return (a*a + b*b - c*c) / (2 * a*b);
}
double maximumAngleCos(double x1, double y1, double z1, double x2, double y2, double z2, double x3, double y3, double z3)
{
	// max cos
	double dis1 = get3DDist(x1, y1, z1, x2, y2, z2);
	double dis2 = get3DDist(x1, y1, z1, x3, y3, z3);
	double dis3 = get3DDist(x2, y2, z2, x3, y3, z3);
	double cos1 = getCos(dis1, dis2, dis3);
	double cos2 = getCos(dis2, dis1, dis3);
	double cos3 = getCos(dis3, dis1, dis2);
	return max(max(cos1, cos2), cos3);
}

bool equal(double a, double b)
{
	return fabs(a - b) < 1e-8;
}

int main(int argc, char* argv[])
{
	/* parse meta data  */

	FILE* fin = fopen(argv[1], "r");
	FILE* fout = fopen(argv[2], "w");

	FILE* ftriangle = fopen(argv[3], "w");

	fscanf(fin, "%*s %d", &ncols);
	fscanf(fin, "%*s %d", &nrows);
	fscanf(fin, "%*s %lf", &xllcorner);
	fscanf(fin, "%*s %lf", &yllcorner);
	fscanf(fin, "%*s %lf", &cellsize);
	fscanf(fin, "%*s %lf", &NODATA_value);

	printf("%d %d %lf %lf %lf %lf\n", ncols, nrows, xllcorner, yllcorner, cellsize, NODATA_value);

	// http://en.wikipedia.org/wiki/Esri_grid
	data.clear();
	int global_id = 0;
	for (int i = 0; i < nrows; i++)
	{
		data.push_back(vector<Point>());
		for (int j = 0; j < ncols; j++)
		{
			double z;
			fscanf(fin, "%lf", &z);
			data[i].push_back(Point(z, equal(z, NODATA_value) ? -1 : global_id++));
		}
	}

	// add grid neighbor

	for (int i = 0; i < nrows; i++)
	{
		for (int j = 0; j < ncols; j++)
		{
			if (data[i][j].id == -1) continue;
			for (int k = 0; k < 8; k += 2)
			{
				int ni = i + dy[k];
				int nj = j + dx[k];
				if (exist(ni, nj))
				{
					data[i][j].setNb(k);
				}
			}
		}
	}

	// triangulation


	for (int i = 0; i < nrows - 1; i++)
	{
		for (int j = 0; j < ncols - 1; j++)
		{

			double z = data[i][j].z;
			double x = getX(j), y = getY(i);


			int ri = i, rj = j + 1;
			double rx = getX(rj), ry = getY(ri), rz = data[ri][rj].z;
			int di = i + 1, dj = j;
			double dx = getX(dj), dy = getY(di), dz = data[di][dj].z;
			int mi = i + 1, mj = j + 1;
			double mx = getX(mj), my = getY(mi), mz = data[mi][mj].z;


			bool bi = exist(i, j), br = exist(ri, rj), bd = exist(di, dj), bm = exist(mi, mj);

			if (bi == false)
			{
				if (br && bd && bm)
				{
					data[ri][rj].setNb(5);
					data[di][dj].setNb(1);
					vt.push_back(Triangle(data[ri][rj].id, data[mi][mj].id, data[di][dj].id));
				}
				continue;
			}

			/*
			4 cases to add k:

			. --r
			|\
			| \
			|  \
			d   m

			1) r, d, m
			2) r, d
			3) r, m
			4) d, m

			*/

			if (br && bd && bm)
			{

				/* \ */
				double ag1_cos = max(maximumAngleCos(x, y, z, dx, dy, dz, mx, my, mz),
					maximumAngleCos(x, y, z, rx, ry, rz, mx, my, mz));

				/* / */
				double ag2_cos = max(maximumAngleCos(x, y, z, dx, dy, dz, rx, ry, rz),
					maximumAngleCos(mx, my, mz, dx, dy, dz, rx, ry, rz));

				if (ag1_cos <= ag2_cos) // ag1 >= ag2
				{
					data[i][j].setNb(3);
					data[mi][mj].setNb(7);
					vt.push_back(Triangle(data[i][j].id, data[ri][rj].id, data[mi][mj].id));
					vt.push_back(Triangle(data[i][j].id, data[mi][mj].id, data[di][dj].id));
				}
				else
				{
					data[ri][rj].setNb(5);
					data[di][dj].setNb(1);
					vt.push_back(Triangle(data[i][j].id, data[ri][rj].id, data[di][dj].id));
					vt.push_back(Triangle(data[ri][rj].id, data[mi][mj].id, data[di][dj].id));
				}
			}
			else if (br && bd)
			{
				data[ri][rj].setNb(5);
				data[di][dj].setNb(1);
				vt.push_back(Triangle(data[i][j].id, data[ri][rj].id, data[di][dj].id));
			}
			else if (br && bm)
			{
				data[i][j].setNb(3);
				data[mi][mj].setNb(7);
				vt.push_back(Triangle(data[i][j].id, data[ri][rj].id, data[mi][mj].id));
			}
			else if (bd && bm)
			{
				data[i][j].setNb(3);
				data[mi][mj].setNb(7);
				vt.push_back(Triangle(data[i][j].id, data[mi][mj].id, data[di][dj].id));
			}
		}
	}

	for (int i = 0; i < nrows; i++)
	{
		for (int j = 0; j < ncols; j++)
		{
			if (data[i][j].id != -1)
			{
				int id = data[i][j].id;
				double z = data[i][j].z;
				double x = getX(j), y = getY(i);
				int nb = data[i][j].countBit();
				fprintf(fout, "%d %.6lf %.6lf %.6lf\t%d ", id, x, y, z, nb);

				for (int k = 0; k < 8; k++)
				{
					if ((1 << k) & data[i][j].nb)
					{
						int ni = i + dy[k];
						int nj = j + dx[k];
						int nid = data[ni][nj].id;
						double nx = getX(nj), ny = getY(ni);
						double nz = data[ni][nj].z;
						double dis = get3DDist(x, y, z, nx, ny, nz);
						fprintf(fout, "%d %.6lf ", nid, dis);
					}
				}
				fprintf(fout, "\n");
			}
		}
	}
	/*
	sort(vt.begin(), vt.end());
	vt.erase(unique(vt.begin(), vt.end()), vt.end());
	*/
	for (int i = 0; i < vt.size(); i++)
	{
		fprintf(ftriangle, "%d %d %d\n", vt[i].x, vt[i].y, vt[i].z);
	}


	fclose(ftriangle);
	fclose(fout);
	fclose(fin);
	return 0;
}
