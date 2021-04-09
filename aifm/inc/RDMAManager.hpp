#pragma once

extern "C" {
#include <runtime/tcp.h>
#include <runtime/sync.h>
}

#include <infiniband/verbs.h>
#include <unistd.h>
#include "helpers.hpp"


#define FORCE_INLINE inline __attribute__((always_inline))

#define MAX_POLL_CQ_TIMEOUT 2000	// poll CQ timeout in millisec (2 seconds)
#define MSG "SEND operation "
#define RDMAMSGR "RDMA read operation "
#define RDMAMSGW "RDMA write operation"
#define MSG_SIZE (strlen(MSG) + 1)

namespace far_memory {
	/* structure of test parameters */
	struct config_t
	{
		const char	*dev_name;	/* IB device name */
		char		*server_name;	/* server host name */
		uint32_t	tcp_port;	/* server TCP port */
		int		ib_port;	/* local IB port to work with */
		int		gid_idx;	/* gid index to use */
	};
	
	/* structure to exchange data which is needed to connect the QPs */
	struct cm_con_data_t
	{
		/*uint32_t	qp_num;*/		/* QP number */
		uint16_t	lid;		/* LID of the IB port */
		uint8_t		gid[16]; 	/* gid */
	} __attribute__((packed));

	/* structure to exchange memory region data */
	struct mr_data_t
	{
		uint64_t	addr;		/* Buffer address */
		uint32_t	rkey;		/* Remote key */
		size_t		len;		/* buffer length */
	} __attribute__((packed));
	
	struct mr_local_t
	{
		struct ibv_mr	*mr;		/* Local MR */
		spinlock_t	lock;		/* SpinLock for sync */
	};

	/* structure of system resources */
	struct resources
	{
		struct ibv_device_attr	device_attr;		/* Device attributes */
		struct ibv_port_attr	port_attr;		/* IB port attributes */
		struct cm_con_data_t	remote_props;		/* values to connect to remote side */
		struct ibv_context	*ib_ctx;		/* device handle */
		struct ibv_pd		*pd;			/* PD handle */
		struct ibv_cq		*cq[helpers::kNumCPUs];	/* CQ handle */
		struct ibv_qp		*qp[helpers::kNumCPUs];	/* QP handle */
	};


	class RDMAManager {
	private:
		tcpconn_t		*remote_master_;
		int			sockfd;		/* TCP socket file descriptor */
		struct resources	res;
		struct config_t		config;
	public:
		/*RDMAManager():sockfd(0), config.dev_name(NULL), config.server_name(NULL), config.tcp_port(19875), config.gid_idx(0);*/
		RDMAManager(char* servername, char* devname, int port, int ibport, int gid_idx);
		RDMAManager():sockfd(-1){
			config.dev_name =	"mlx5_0";
			config.server_name =	NULL;
			config.tcp_port =	19785;
			config.ib_port =	1;
			config.gid_idx =	0;};
		~RDMAManager(){
			resources_destroy();};
		void	set_server(char* server){
			config.server_name = server;};
		void	set_dev(char* dev){
			config.dev_name = dev;};
		void	set_ib_port(int port){
			config.ib_port = port;};
		/*uint64_t get_buff_addr(){*/
			/*return reinterpret_cast<uint64_t>(res.buf);}*/
		void	set_tcpconn(tcpconn_t *c){
			remote_master_ = c;}
		tcpconn_t* get_tcpconn(void){
			return remote_master_;}
		struct ibv_mr* reg_addr(uint64_t addr, uint64_t len, bool isLocal);
		
		
		void	tcp_connect(netaddr raddr);
		void	tcp_sync_data(int xfer_size, char *local_data, char *remote_data);
		
		int	resources_destroy(void);
		int	resources_create(int cq_size, int cores);
		int	connect_qp(uint8_t cores);

		int	modify_qp_to_init(struct ibv_qp *qp);
		int	modify_qp_to_rtr(struct ibv_qp *qp, uint32_t remote_qpn, uint16_t dlid, uint8_t *dgid);
		int	modify_qp_to_rts(struct ibv_qp *qp);
	
		int	post_send(uint8_t core, int opcode, uint64_t local_addr, uint32_t len, uint32_t lkey
				, struct mr_data_t *remote_mr, uint64_t remote_addr_offset);
		int	post_receive(uint8_t core, uintptr_t addr, uint32_t len, uint32_t lkey);
		int	poll_completion(uint8_t core);
	};
}
