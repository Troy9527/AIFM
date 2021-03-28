extern "C" {	
#include <net/ip.h>
}

#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <endian.h>
#include <byteswap.h>
#include <inttypes.h>
#include <sys/time.h>

#include "RDMAManager.hpp"
#include "helpers.hpp"
#ifndef DEBUG
#define DEBUG 1
#endif

#if __BYTE_ORDER == __LITTLE_ENDIAN
static inline uint64_t htonll(uint64_t x) { return bswap_64(x); }
static inline uint64_t ntohll(uint64_t x) { return bswap_64(x); }
static inline uint64_t htonl(uint32_t x) { return bswap_32(x); }
static inline uint64_t ntohl(uint32_t x) { return bswap_32(x); }
static inline uint64_t htons(uint16_t x) { return bswap_16(x); }
static inline uint64_t ntohs(uint16_t x) { return bswap_16(x); }
#elif __BYTE_ORDER == __BIG_ENDIAN
static inline uint64_t htonll(uint64_t x) { return x; }
static inline uint64_t ntohll(uint64_t x) { return x; }
static inline uint64_t htonl(uint32_t x) { return x; }
static inline uint64_t ntohl(uint32_t x) { return x; }
static inline uint64_t htons(uint16_t x) { return x; }
static inline uint64_t ntohs(uint16_t x) { return x; }
#else
#error __BYTE_ORDER is neither __LITTLE_ENDIAN nor __BIG_ENDIAN
#endif

namespace far_memory {

	
RDMAManager::RDMAManager(char* servername, char* devname, 
		int port, int ibport, int gid_idx):sockfd(-1){
	config.dev_name = devname;		
	config.server_name = servername;		
	config.tcp_port = port;		
	config.ib_port = ibport;		
	config.gid_idx = gid_idx;

	memset(&res, 0, sizeof(struct resources));
}


void RDMAManager::tcp_connect(netaddr raddr){
	netaddr laddr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0), .port = 0};
	BUG_ON(tcp_dial(laddr, raddr, &remote_master_) != 0);
}

void RDMAManager::tcp_sync_data(int xfer_size, char *local_data, char *remote_data){
	helpers::tcp_write_until(remote_master_, local_data, xfer_size);
	helpers::tcp_read_until(remote_master_, remote_data, xfer_size);
}

int RDMAManager::sock_connect(void){
	/*struct addrinfo		*resolved_addr = NULL;
	struct addrinfo		*iterator;
	char			service[6];
	int			listenfd = 0;
	struct addrinfo		hints ={
					.ai_flags = AI_PASSIVE,
					.ai_family = AF_INET,
					.ai_socktype = SOCK_STREAM};

	if (sprintf(service, "%d", config.tcp_port) < 0){
		std::cerr << "sprintf error" << std::endl;
		return -1;
	}

	sockfd = getaddrinfo(config.server_name, service, &hints, &resolved_addr);
	if (sockfd < 0){
		std::cerr << "getaddrinfo error" << std::endl;
		return -1;
	}


	for (iterator = resolved_addr; iterator; iterator = iterator->ai_next)
	{
		sockfd = socket(iterator->ai_family, iterator->ai_socktype, iterator->ai_protocol);
		if (sockfd >= 0)
		{
			if (config.server_name)
			{
				if (connect(sockfd, iterator->ai_addr, iterator->ai_addrlen) < 0)
				{
					std::cerr << "connect failed" << std::endl;
					close(sockfd);
					sockfd = -1;
				}
            		}
			else
			{
				listenfd = sockfd;
				if (bind(listenfd, iterator->ai_addr, iterator->ai_addrlen) < 0){
					std::cerr << "bind failed" << std::endl;
					return -1;
				}

				listen(listenfd, 1);
				sockfd = accept(listenfd, NULL, 0);
			}
		}
	}
		
	if (listenfd)
		close(listenfd);
	if (resolved_addr)
		freeaddrinfo(resolved_addr);

	return sockfd;*/
}


int RDMAManager::sock_sync_data(int xfer_size, char *local_data, char *remote_data){
	/*int	rc;
	int	read_bytes = 0;
	int	total_read_bytes = 0;
	
	rc = write(sockfd, local_data, xfer_size);
	if (rc < xfer_size)
		fprintf(stderr, "Failed writing data during sock_sync_data\n");
	else
		rc = 0;
	while (!rc && total_read_bytes < xfer_size)
	{
		read_bytes = read(sockfd, remote_data, xfer_size);
		if (read_bytes > 0)
			total_read_bytes += read_bytes;
		else
			rc = read_bytes;
	}
	return rc;*/
}

int RDMAManager::resources_create(int cq_size, int memory_size){
	struct ibv_device		**dev_list = NULL;
	struct ibv_qp_init_attr		qp_init_attr;
	struct ibv_device		*ib_dev = NULL;
	int				i, mr_flags = 0, num_devices, rc = 0;

	
	/*if(sock_connect() < 0){*/
		/*std::cerr << "sock_connect failed" << std::endl;*/
		/*rc = 1;*/
		/*goto resources_create_exit;*/
	/*}*/


	/* get device names in the system */
	dev_list = ibv_get_device_list(&num_devices);
	if (!dev_list){
		std::cerr << "failed to get IB devices list\n" << std::endl;
		rc = 1;
		goto resources_create_exit;
	}

	/* if there isn't any IB device in host */
	if (!num_devices){
		std::cerr << "found " << num_devices << " device(s)" << std::endl;
		rc = 1;
		goto resources_create_exit;
	}
#ifdef DEBUG
	std::cout << "found " << num_devices << " device(s)" << std::endl;
#endif
	
	/* search for the specific device we want to work with */
	for (i = 0; i < num_devices; i++){
		if (!config.dev_name){
			config.dev_name = strdup(ibv_get_device_name(dev_list[i]));
#ifdef DEBUG
			std::cout << "device not specified, using first one found: " << config.dev_name << std::endl;
#endif
		}
		if (!strcmp(ibv_get_device_name(dev_list[i]), config.dev_name)){
			ib_dev = dev_list[i];
			break;
		}
	}

	/* if the device wasn't found in host */
	if (!ib_dev){
		std::cerr << "IB device " << config.dev_name << " wasn't found" << std::endl;
		rc = 1;
		goto resources_create_exit;
	}

	/* get device handle */
	res.ib_ctx = ibv_open_device(ib_dev);
	if (!res.ib_ctx){
		std::cerr << "failed to open device " << config.dev_name << std::endl;
		rc = 1;
		goto resources_create_exit;
	}
	/* We are now done with device list, free it */
	ibv_free_device_list(dev_list);
	dev_list = NULL;
	ib_dev = NULL;

	/* query port properties */
	if (ibv_query_port(res.ib_ctx, config.ib_port, &res.port_attr)){
		std::cerr << "ibv_query_port on port " << config.ib_port << " failed" << std::endl;
		rc = 1;
		goto resources_create_exit;
	}
	
	/* allocate Protection Domain */
	res.pd = ibv_alloc_pd(res.ib_ctx);
	if (!res.pd){
		std::cerr << "ibv_alloc_pd failed" << std::endl;
		rc = 1;
		goto resources_create_exit;
	}
	
	/* each side will send only one WR, so Completion Queue with 1 entry is enough */
	cq_size = 1;
	res.cq = ibv_create_cq(res.ib_ctx, cq_size, NULL, NULL, 0);
	if (!res.cq){
		std::cerr << "failed to create CQ with " << cq_size << " entries" << std::endl;
		rc = 1;
		goto resources_create_exit;
	}
	
	
	/* allocate the memory buffer that will hold the data */
	/*if (memory_size != -1){
		res.msize = memory_size;
		res.buf = (char *)malloc(res.msize);
		if (!res.buf){
			fprintf(stderr, "failed to malloc %Zu bytes to memory buffer\n", static_cast<size_t>(res.msize));
			rc = 1;
			goto resources_create_exit;
		}
		memset(res.buf, 0, res.msize);
		
		mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
		res.mr = ibv_reg_mr(res.pd, res.buf, res.msize, mr_flags);
		if (!res.mr){
			fprintf(stderr, "ibv_reg_mr failed with mr_flags=0x%x\n", mr_flags);
			rc = 1;
			goto resources_create_exit;
		}
#ifdef DEBUG
		fprintf(stdout, "MR was registered with addr=%p, lkey=0x%x, rkey=0x%x, flags=0x%x\n",
			res.buf, res.mr->lkey, res.mr->rkey, mr_flags);
#endif
	}*/
	
	/* create the Queue Pair */
	memset(&qp_init_attr, 0, sizeof(qp_init_attr));
	qp_init_attr.qp_type = IBV_QPT_RC;
	qp_init_attr.sq_sig_all = 1;
	qp_init_attr.send_cq = res.cq;
	qp_init_attr.recv_cq = res.cq;
	qp_init_attr.cap.max_send_wr = 1;
	qp_init_attr.cap.max_recv_wr = 1;
	qp_init_attr.cap.max_send_sge = 1;
	qp_init_attr.cap.max_recv_sge = 1;
	res.qp = ibv_create_qp(res.pd, &qp_init_attr);
	if (!res.qp){
		fprintf(stderr, "failed to create QP\n");
		rc = 1;
		goto resources_create_exit;
	}
#ifdef DEBUG
	fprintf(stdout, "QP was created, QP number=0x%x\n", res.qp->qp_num);
#endif

resources_create_exit:
	if (rc){
		/* Error encountered, cleanup */
		if (res.qp){
			ibv_destroy_qp(res.qp);
			res.qp = NULL;
		}
		/*if (res.mr){*/
			/*ibv_dereg_mr(res.mr);*/
			/*res.mr = NULL;*/
		/*}*/
		/*if (res.buf){*/
			/*free(res.buf);*/
			/*res.buf = NULL;*/
		/*}*/
		if (res.cq){
			ibv_destroy_cq(res.cq);
			res.cq = NULL;
		}
		if (res.pd){
			ibv_dealloc_pd(res.pd);
			res.pd = NULL;
		}
		if (res.ib_ctx){
			ibv_close_device(res.ib_ctx);
			res.ib_ctx = NULL;
		}
		if (dev_list){
			ibv_free_device_list(dev_list);
			dev_list = NULL;
		}
		/*if (sockfd >= 0){*/
			/*close(sockfd);*/
			/*sockfd = -1;*/
		/*}*/
	}

	return rc;

}

int RDMAManager::connect_qp(void){
	struct cm_con_data_t	local_con_data;
	struct cm_con_data_t	remote_con_data;
	struct cm_con_data_t	tmp_con_data;
	int			rc = 0;
	char			temp_char;
	union ibv_gid		my_gid;
	char			t[2] = "Q";

	if (config.gid_idx >= 0){
		rc = ibv_query_gid(res.ib_ctx, config.ib_port, config.gid_idx, &my_gid);
		if (rc){
			fprintf(stderr, "could not get gid for port %d, index %d\n", config.ib_port, config.gid_idx);
			return rc;
		}
	}
	else
		memset(&my_gid, 0, sizeof my_gid);

	
	/* exchange using TCP sockets info required to connect QPs */
	/*local_con_data.addr = htonll((uintptr_t)res.buf);*/
	/*local_con_data.rkey = htonl(res.mr->rkey);*/
	local_con_data.qp_num = htonl(res.qp->qp_num);
	local_con_data.lid = htons(res.port_attr.lid);
	memcpy(local_con_data.gid, &my_gid, 16);
#ifdef DEBUG
	fprintf(stdout, "\nLocal LID = 0x%x\n", res.port_attr.lid);
#endif
	tcp_sync_data(sizeof(struct cm_con_data_t), (char *)&local_con_data, (char *)&tmp_con_data);
	/*if (tcp_sync_data(sizeof(struct cm_con_data_t), (char *)&local_con_data, (char *)&tmp_con_data) < 0){*/
		/*fprintf(stderr, "failed to exchange connection data between sides\n");*/
		/*rc = 1;*/
		/*goto connect_qp_exit;*/
	/*}*/

	/*remote_con_data.addr = ntohll(tmp_con_data.addr);*/
	/*remote_con_data.rkey = ntohl(tmp_con_data.rkey);*/
	remote_con_data.qp_num = ntohl(tmp_con_data.qp_num);
	remote_con_data.lid = ntohs(tmp_con_data.lid);
	memcpy(remote_con_data.gid, tmp_con_data.gid, 16);
	
	/* save the remote side attributes, we will need it for the post SR */
	res.remote_props = remote_con_data;
#ifdef DEBUG
	/*fprintf(stdout, "Remote address = 0x%" PRIx64 "\n", remote_con_data.addr);*/
	/*fprintf(stdout, "Remote rkey = 0x%x\n", remote_con_data.rkey);*/
	fprintf(stdout, "Remote QP number = 0x%x\n", remote_con_data.qp_num);
	fprintf(stdout, "Remote LID = 0x%x\n", remote_con_data.lid);
	if (config.gid_idx >= 0){
		uint8_t *p = remote_con_data.gid;
		fprintf(stdout, "Remote GID =%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n ",p[0],
				  p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11], p[12], p[13], p[14], p[15]);
	}
#endif

	/* modify the QP to init */
	rc = modify_qp_to_init(res.qp);
	if (rc){
		fprintf(stderr, "change QP state to INIT failed\n");
		goto connect_qp_exit;
	}
	
	/* modify the QP to RTR */
	rc = modify_qp_to_rtr(res.qp, remote_con_data.qp_num, remote_con_data.lid, remote_con_data.gid);
	if (rc){
		fprintf(stderr, "failed to modify QP state to RTR\n");
		goto connect_qp_exit;
	}
	
	/* modify the QP to RTS */
	rc = modify_qp_to_rts(res.qp);
	if (rc){
		fprintf(stderr, "failed to modify QP state to RTS\n");
		goto connect_qp_exit;
	}
#ifdef DEBUG
	fprintf(stdout, "QP state was change to RTS\n");
#endif

	/* sync to make sure that both sides are in states that they can connect to prevent packet loose */
	tcp_sync_data(1, t, &temp_char);


connect_qp_exit:
	return rc;
}


int RDMAManager::modify_qp_to_init(struct ibv_qp *qp){
	struct ibv_qp_attr	attr;
	int			flags;
	int			rc;

	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_INIT;
	attr.port_num = config.ib_port;
	attr.pkey_index = 0;
	attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
	flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
	
	rc = ibv_modify_qp(qp, &attr, flags);
	if (rc)
		fprintf(stderr, "failed to modify QP state to INIT\n");
	return rc;
}

int RDMAManager::modify_qp_to_rtr(struct ibv_qp *qp, uint32_t remote_qpn, uint16_t dlid, uint8_t *dgid){
	struct ibv_qp_attr	attr;
	int			flags;
	int			rc;

	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_RTR;
	attr.path_mtu = IBV_MTU_256;
	attr.dest_qp_num = remote_qpn;
	attr.rq_psn = 0;
	attr.max_dest_rd_atomic = 1;
	attr.min_rnr_timer = 0x12;
	attr.ah_attr.is_global = 0;
	attr.ah_attr.dlid = dlid;
	attr.ah_attr.sl = 0;
	attr.ah_attr.src_path_bits = 0;
	attr.ah_attr.port_num = config.ib_port;
	if (config.gid_idx >= 0){
		attr.ah_attr.is_global = 1;
		attr.ah_attr.port_num = 1;
		memcpy(&attr.ah_attr.grh.dgid, dgid, 16);
		attr.ah_attr.grh.flow_label = 0;
		attr.ah_attr.grh.hop_limit = 1;
		attr.ah_attr.grh.sgid_index = config.gid_idx;
		attr.ah_attr.grh.traffic_class = 0;
	}
	flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
			IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
	
	rc = ibv_modify_qp(qp, &attr, flags);
	if (rc)
		fprintf(stderr, "failed to modify QP state to RTR, ERRNO = %d\n", rc);
	return rc;
}

int RDMAManager::modify_qp_to_rts(struct ibv_qp *qp){
	struct ibv_qp_attr	attr;
	int			flags;
	int			rc;
	
	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_RTS;
	attr.timeout = 0x12;
	attr.retry_cnt = 6;
	attr.rnr_retry = 0;
	attr.sq_psn = 0;
	attr.max_rd_atomic = 1;
	flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
			IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
	
	rc = ibv_modify_qp(qp, &attr, flags);
	if (rc)
		fprintf(stderr, "failed to modify QP state to RTS, ERRNO = %d\n", rc);
	return rc;
}


int RDMAManager::post_send(int opcode, uint64_t local_addr, int len, uint32_t lkey
		, struct mr_data_t remote_mr, int remote_addr_offset){
	struct ibv_send_wr	sr;
	struct ibv_sge		sge;
	struct ibv_send_wr	*bad_wr = NULL;
	int			rc;
	
	/* prepare the scatter/gather entry */
	memset(&sge, 0, sizeof(sge));
	sge.addr = (uintptr_t)local_addr;
	sge.length = len;
	sge.lkey = lkey;
	
	/* prepare the send work request */
	memset(&sr, 0, sizeof(sr));
	sr.next = NULL;
	sr.wr_id = 0;
	sr.sg_list = &sge;
	sr.num_sge = 1;
	sr.opcode = static_cast<ibv_wr_opcode>(opcode);
	sr.send_flags = IBV_SEND_SIGNALED;
	if (opcode != IBV_WR_SEND){
		sr.wr.rdma.remote_addr = remote_mr.addr + remote_addr_offset;
		sr.wr.rdma.rkey = remote_mr.rkey;
	}
	
	/* there is a Receive Request in the responder side, so we won't get any into RNR flow */
	rc = ibv_post_send(res.qp, &sr, &bad_wr);
	if (rc)
		fprintf(stderr, "failed to post SR\n");
	else{
		switch (opcode){
		case IBV_WR_SEND:
#ifdef DEBUG
			fprintf(stdout, "Send Request was posted\n");
#endif
			break;
		case IBV_WR_RDMA_READ:
#ifdef DEBUG
			fprintf(stdout, "RDMA Read Request was posted\n");
#endif
			break;
		case IBV_WR_RDMA_WRITE:
#ifdef DEBUG
			fprintf(stdout, "RDMA Write Request was posted\n");
#endif
			break;
		default:
#ifdef DEBUG
			fprintf(stdout, "Unknown Request was posted\n");
#endif
			break;
		}
	}
	return rc;
}

int RDMAManager::post_receive(uintptr_t addr, int len, uint32_t lkey){
	struct ibv_recv_wr	rr;
	struct ibv_sge		sge;
	struct ibv_recv_wr	*bad_wr;
	int			rc;
	
	/* prepare the scatter/gather entry */
	memset(&sge, 0, sizeof(sge));
	sge.addr = (uintptr_t)addr;
	sge.length = len;
	sge.lkey = lkey;
	
	/* prepare the receive work request */
	memset(&rr, 0, sizeof(rr));
	rr.next = NULL;
	rr.wr_id = 0;
	rr.sg_list = &sge;
	rr.num_sge = 1;
	
	/* post the Receive Request to the RQ */
	rc = ibv_post_recv(res.qp, &rr, &bad_wr);
	if (rc)
		fprintf(stderr, "failed to post RR\n");
#ifdef DEBUG
	else
		fprintf(stdout, "Receive Request was posted\n");
#endif
	return rc;
}

int RDMAManager::poll_completion(void){
	struct ibv_wc		wc;
	unsigned long		start_time_msec;
	unsigned long		cur_time_msec;
	struct timeval		cur_time;
	int			poll_result;
	int			rc = 0;
	
	/* poll the completion for a while before giving up of doing it .. */
	gettimeofday(&cur_time, NULL);
	start_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
	do{
		poll_result = ibv_poll_cq(res.cq, 1, &wc);
		gettimeofday(&cur_time, NULL);
		cur_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
	} while ((poll_result == 0) && ((cur_time_msec - start_time_msec) < MAX_POLL_CQ_TIMEOUT));
	
	if (poll_result < 0){
		/* poll CQ failed */
		fprintf(stderr, "poll CQ failed\n");
		rc = 1;
	}
	else if (poll_result == 0){ /* the CQ is empty */
		fprintf(stderr, "completion wasn't found in the CQ after timeout\n");
		rc = 1;
	}
	else{
		/* CQE found */
#ifdef DEBUG
		fprintf(stdout, "completion was found in CQ with status 0x%x\n", wc.status);
#endif
		/* check the completion status (here we don't care about the completion opcode */
		if (wc.status != IBV_WC_SUCCESS){
			fprintf(stderr, "got bad completion with status: 0x%x, vendor syndrome: 0x%x\n", wc.status,wc.vendor_err);
			rc = 1;
		}
	}
	return rc;
}

int RDMAManager::resources_destroy(void){
	int rc = 0;
	if (res.qp)
		if (ibv_destroy_qp(res.qp)){
			fprintf(stderr, "failed to destroy QP\n");
			rc = 1;
		}
	/*if (res.mr)*/
		/*if (ibv_dereg_mr(res.mr)){*/
			/*fprintf(stderr, "failed to deregister MR\n");*/
			/*rc = 1;*/
		/*}*/
	/*if (res.buf)*/
		/*free(res.buf);*/
	if (res.cq)
		if (ibv_destroy_cq(res.cq)){
			fprintf(stderr, "failed to destroy CQ\n");
			rc = 1;
		}
	if (res.pd)
		if (ibv_dealloc_pd(res.pd)){
			fprintf(stderr, "failed to deallocate PD\n");
			rc = 1;
		}
	if (res.ib_ctx)
		if (ibv_close_device(res.ib_ctx)){
			fprintf(stderr, "failed to close device context\n");
			rc = 1;
		}
	/*if (sockfd >= 0)*/
		/*if (close(sockfd)){*/
			/*fprintf(stderr, "failed to close socket\n");*/
			/*rc = 1;*/
		/*}*/
	
	tcp_close(remote_master_);
	return rc;
}

struct ibv_mr* RDMAManager::reg_addr(uint64_t addr, int len){
	struct ibv_mr	*ret = NULL;
	int		mr_flags = 0;
	

	mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
	ret = ibv_reg_mr(res.pd, reinterpret_cast<char*>(addr), len, mr_flags);
	if (!ret){
		fprintf(stderr, "ibv_reg_mr failed with mr_flags=0x%x\n", mr_flags);
		return NULL;
	}
#ifdef DEBUG
	fprintf(stdout, "MR was registered with addr=%p, lkey=0x%x, rkey=0x%x, flags=0x%x\n",
			ret->addr, ret->lkey, ret->rkey, mr_flags);
#endif

	
	return	ret;

}

}
